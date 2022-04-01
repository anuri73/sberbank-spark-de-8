"src/main/resources/autousers.json"
"src/main/resources/logs"
"src/main/resources/laba02_domains.csv"


spark-submit \
--master "yarn" \
--executor-cores 2 \
--executor-memory 2g \
--num-executors 10 \
Lab2-assembly-0.1.jar autousers.json /labs/laba02/logs laba02_domains



spark-submit \
--master "yarn" \
--executor-cores 2 \
--executor-memory 2g \
--num-executors 10 \
Lab2-assembly-0.1.jar hdfs://spark-master-3.newprolab.com:8020/user/urmat.zhenaliev/laba02/autousers.json hdfs://spark-master-3.newprolab.com:8020/user/urmat.zhenaliev/laba02/logs laba02_domains


