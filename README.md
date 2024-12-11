# Apache Flink 1.19.1 `KeyedStateBootstrapFunction` hangs on exception reproducer


Reproducer for [FLINK-37639](https://issues.apache.org/jira/browse/FLINK-37639)

```sh
./gradlew clean assemble

# spawn flink cluster

flink run \
    -D pipeline.max-parallelism=10 \
    -D parallelism.default=2 \
    -D execution.runtime-mode=BATCH \
    -D execution.batch-shuffle-mode=ALL_EXCHANGES_PIPELINED \
    -D jobmanager.scheduler=Default \
    build/libs/flink-bootstrap-function-issue-1.0-SNAPSHOT-all.jar \
    --destination /path/to/write/the/savepoint/
```
