To run application run the following command with arguments

```bash
./bin/spark-submit \
  --class <main-class> \
  --master <master-url> \
  --deploy-mode <deploy-mode> \
  --conf <key>=<value> \
  ... # other options
  Lab1-assembly-0.1.jar \
  {{autoUsersJsonPath}} \
  {{logsPath}} \
  {{resultPath}}
```

**Arguments:**

* autoUsersJsonPath - Movie id
* logsPath - Source file path
* resultPath - Output result path

Example

```bash
spark-submit \
--master "yarn" \
--executor-cores 2 \
--executor-memory 2g \
--num-executors 10 \
Lab2-assembly-0.1.jar /labs/laba02/autousers.json /labs/laba02/logs laba02_domains
```

