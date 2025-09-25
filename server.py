import os
import json
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ASCENDING, ReturnDocument

from contextlib import asynccontextmanager
# --------------------
# Config
# --------------------
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017") # localhost:27017
MONGO_DB = os.getenv("MONGO_DB", "searchdb")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "search_cache")
DOTNET_BASE_URL = os.getenv("DOTNET_BASE_URL", "http://dotnet:5014")  # localhost:5014
CACHE_TTL_MINUTES = int(os.getenv("CACHE_TTL_MINUTES", "10"))


mongo_client: AsyncIOMotorClient = AsyncIOMotorClient(
    MONGO_URI,
    tz_aware=True,
    tzinfo=timezone.utc,
)
db = mongo_client[MONGO_DB]
cache = db[MONGO_COLLECTION]

http = httpx.AsyncClient(timeout=httpx.Timeout(20.0))

# --------------------
# Housekeeping (1x por hora)
# --------------------
scheduler = AsyncIOScheduler()

async def cleanup_job():
    threshold = utcnow() - timedelta(minutes=CACHE_TTL_MINUTES)
    result = await cache.delete_many({"updated_at": {"$lt": threshold}})
    # opcional: logar result.deleted_count

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await ensure_indexes()
    scheduler.add_job(cleanup_job, "interval", hours=1, id="cache_cleanup", replace_existing=True)
    scheduler.start()

    yield  # <-- control passes to FastAPI while app runs

    # Shutdown
    await http.aclose()
    scheduler.shutdown()

app = FastAPI(title="Python Search Cache Service", version="1.0.0", lifespan=lifespan)

# --------------------
# Models
# --------------------
class AdvancedSearchRequest(BaseModel):
    servico: Optional[str] = None
    regiao: Optional[str] = None
    faixaPrecoMax: Optional[float] = None
    avaliacoesMinimas: Optional[int] = None
    # Campos adicionais livres
    extra: Dict[str, Any] = Field(default_factory=dict)

# --------------------
# Utils
# --------------------
def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def normalize_filters(filters: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normaliza filtros para chavear o cache:
    - strings em lower/trim,
    - remove None,
    - ordena chaves (feito no dump).
    """
    def norm(v):
        if isinstance(v, str):
            return v.strip().lower()
        if isinstance(v, dict):
            return {k: norm(v[k]) for k in v if v[k] is not None}
        if isinstance(v, list):
            return [norm(x) for x in v if x is not None]
        return v
    return norm({k: v for k, v in filters.items() if v is not None})

def cache_key_from_filters(filters: Dict[str, Any]) -> str:
    normalized = normalize_filters(filters)
    dump = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(dump.encode("utf-8")).hexdigest()

async def ensure_indexes():
    await cache.create_index([("key", ASCENDING)], unique=True)
    await cache.create_index([("updated_at", ASCENDING)])

async def get_cached_or_refresh(
    filters: Dict[str, Any],
    dotnet_path: str,
    dotnet_method: str = "GET",
    body: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Busca no cache; se ausente ou expirado (> TTL), chama .NET, salva e retorna.
    """
    key = cache_key_from_filters(filters)
    now = utcnow()
    ttl_delta = timedelta(minutes=CACHE_TTL_MINUTES)

    doc = await cache.find_one({"key": key})
    if doc:
        if (now - doc["updated_at"]) <= ttl_delta:
            return doc["payload"]  # cache HIT válido

    # Cache miss ou expirado => chama .NET
    try:
        if dotnet_method == "GET":
            print(f"{DOTNET_BASE_URL}{dotnet_path}")
            r = await http.get(f"{DOTNET_BASE_URL}{dotnet_path}", params=filters)
        else:
            r = await http.post(f"{DOTNET_BASE_URL}{dotnet_path}", json=body or filters)
        r.raise_for_status()
        payload = r.json()
    except httpx.HTTPError as e:
        # se havia cache expirado, podemos retornar o expirado como fallback?
        # Por padrão, retornamos 502 para sinalizar upstream failure.
        raise HTTPException(status_code=502, detail=f"Erro chamando serviço .NET: {str(e)}")

    # upsert do cache
    await cache.find_one_and_update(
        {"key": key},
        {
            "$set": {
                "key": key,
                "filters": normalize_filters(filters),
                "payload": payload,
                "updated_at": now,
            }
        },
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )
    return payload

# --------------------
# Endpoints
# --------------------

@app.get("/ping")
async def ping():
    return JSONResponse(content="pong")

# Equivalente ao GET /busca?termo=...&cidade=...
@app.get("/busca")
async def busca_simples(
    termo: str = Query(...),
    cidade: Optional[str] = Query(None),
):
    filters = {"termo": termo, "cidade": cidade}
    data = await get_cached_or_refresh(filters, dotnet_path="/catalogo/busca", dotnet_method="GET")
    return JSONResponse(content=data)

# Equivalente ao POST /busca/avancada
@app.post("/busca/avancada")
async def busca_avancada(req: AdvancedSearchRequest):
    filters = {
        "servico": req.servico,
        "regiao": req.regiao,
        "faixaPrecoMax": req.faixaPrecoMax,
        "avaliacoesMinimas": req.avaliacoesMinimas,
        **req.extra,
    }
    data = await get_cached_or_refresh(filters, dotnet_path="/catalogo/busca/avancada", dotnet_method="POST", body=filters)
    return JSONResponse(content=data)