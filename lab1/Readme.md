To run application run the following command with arguments

```bash
./bin/spark-submit \
  --class <main-class> \
  --master <master-url> \
  --deploy-mode <deploy-mode> \
  --conf <key>=<value> \
  ... # other options
  Lab1-assembly-0.1.jar \
  {{movieId}} \
  {{sourcePath}} \
  {{resultPath}}
```

**Arguments:**

* movieId - Movie id
* sourcePath - Source file path
* resultPath - Output result path

Example

```bash
spark-submit --master "yarn" --executor-cores 2 --executor-memory 2g --num-executors 10 Lab1-assembly-0.1.jar 202 /labs/laba01/ml-100k/u.data lab01.json
```