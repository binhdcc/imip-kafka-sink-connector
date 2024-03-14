## Imip Kafka Sink Connector

## Some command

```bash
curl -X POST \
    -d @examples/imip-sink-connector.json \
    -H "Content-Type:application/json" \
    http://localhost:8083/connectors

curl -X DELETE \
    http://localhost:8083/connectors/imip-sink-connector
```
