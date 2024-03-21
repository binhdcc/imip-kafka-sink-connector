## Imip Kafka Sink Connector
guava23guava-23.0.jar
spark-3.5.1-bin-hadoop3/jars
## Some command

```bash
curl -X POST \
    -d @examples/imip-sink-connector.json \
    -H "Content-Type:application/json" \
    http://localhost:8083/connectors

curl -X DELETE \
    http://localhost:8083/connectors/imip-sink-connector

curl -X GET \
    http://localhost:8083/connectors
```
