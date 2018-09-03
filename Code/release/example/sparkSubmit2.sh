#$1 - jar
#$2 - класс
#$3 - номер таблицы
export HADOOP_CONF_DIR=/etc/hive/conf
spark2-submit --principal 'sbt-medvedev-ba@DEV.DF.SBRF.RU' --keytab 'sbt-medvedev-ba.keytab' --conf spark.network.timeout=10000000 --conf "spark.executor.extraJavaOptions=-Xss30m" --conf "spark.driver.extraJavaOptions=-Xss30m" --conf "yarn.nodemanager.resource.memory-mb=12000" --conf "spark.dynamicAllolcation.enabled=true" --conf "spark.yarn.appMasterEnv.hive.metastore.uris=thrift://hw2288-02.cloud.dev.df.sbrf.ru:9083" --class $2 --master yarn --deploy-mode cluster --driver-memory 20G --executor-memory 36G --executor-cores 4 --num-executors 48 --verbose $1 $3