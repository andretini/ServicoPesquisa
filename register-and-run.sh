#!/bin/sh
set -e

# Function to register with Consul
register_service() {
  CONTAINER_IP=$(hostname -i)
  echo "Registering with Consul at $CONTAINER_IP:8000..."
  curl -X PUT http://consul:8500/v1/agent/service/register \
    -H "Content-Type: application/json" \
    -d "{
      \"ID\": \"servicopesquisa-$(hostname)\",
      \"Name\": \"servico-busca\",
      \"Address\": \"$CONTAINER_IP\",
      \"Port\": 8000,
      \"Tags\": [\"api\", \"search\"],
      \"Check\": {
        \"HTTP\": \"http://$CONTAINER_IP:8000/ping\",
        \"Interval\": \"10s\",
        \"Timeout\": \"5s\"
      }
    }"
  echo "Service registered!"
}

# Register periodically in background (keep registration alive)
while true; do
  register_service
  sleep 30
done &

# Start the application
exec uvicorn server:app --reload --port=8000 --host=0.0.0.0
