from fastapi import FastAPI
from pymongo import MongoClient
import os

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017/TestePy")
client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)

def serialize_doc(doc: dict) -> dict:
    doc = dict(doc)
    if "_id" in doc:
        doc["_id"] = str(doc["_id"])
    return doc

try:
    client.admin.command("ping")  # verify connection on startup
except Exception as e:
    # If you prefer: just print and let endpoints raise later
    raise RuntimeError(f"Cannot connect to MongoDB: {e}")

db = client["TestePy"]
colecao = db["Cache"]

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "Hello, FastAPI!"}

@app.get("/user")
def read_root():
    return { "users": [serialize_doc(d) for d in colecao.find()] }

@app.post("/user/{name}/{idade}")
def inert_user(name: str, idade: int):
    # Inserir um documento
    colecao.insert_one({"nome": name, "idade": idade})
    return {"message": f"usuário: {name}, inserido com sucesso"}
