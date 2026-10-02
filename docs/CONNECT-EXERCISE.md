# Optional exercise: a FileStream connector to fill the Connect panels

The base demo checks the Connect **worker** metrics. The per-connector and per-task panels of the
*Kafka Connect* dashboard stay empty until a connector runs. This exercise adds a lab-only
FileStreamSource connector, with no external system. It is not a recommendation for log collection.

1. On `connect1`, look for the FileStream JAR shipped with the distribution:

   ```bash
   docker exec cp-otel-connect1 find /usr/share/java -name 'connect-file*.jar'
   ```

2. If a JAR is found, copy it (do not move it) to a dedicated directory such as
   `/usr/share/java/connect_plugins/demo-file/`, readable by the worker user. Then restart the worker:
   `docker exec cp-otel-connect1 systemctl restart confluent-kafka-connect`.
   If no JAR is found, stop here and use a connector that is already installed and approved.

3. Check that `curl -s http://localhost:8083/connector-plugins` lists
   `org.apache.kafka.connect.file.FileStreamSourceConnector`. Create a file `/tmp/demo-input.txt`
   in `connect1`, readable by the worker, with a few lines of text.

4. Create the connector:

   ```bash
   curl --fail-with-body -X PUT http://localhost:8083/connectors/demo-file/config \
     -H 'Content-Type: application/json' --data '{
       "connector.class":"org.apache.kafka.connect.file.FileStreamSourceConnector",
       "tasks.max":"1",
       "file":"/tmp/demo-input.txt",
       "topic":"demo-file-events",
       "key.converter":"org.apache.kafka.connect.storage.StringConverter",
       "value.converter":"org.apache.kafka.connect.storage.StringConverter"
     }'
   ```

5. Add lines to the file. Check `curl -s http://localhost:8083/connectors/demo-file/status`,
   then the topic `demo-file-events` and the *Kafka Connect* dashboard.

`/tmp` is temporary in the lab containers. `up.sh` does not run this exercise, and the tests do not depend on it.
