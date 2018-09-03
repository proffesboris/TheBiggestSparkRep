SPARK_HOME=/opt/cloudera/parcels/SPARK2-2.2.0.cloudera1-1.cdh5.12.0.p0.142354
SPARK_SUBMIT=$SPARK_HOME/bin/spark2-submit
export HADOOP_CONF_DIR=/etc/hive/conf
TIMESTAMP=$(date +%Y%m%d%H%M%S)

$SPARK_SUBMIT \
 --principal "${user}@DF.SBRF.RU" \
 --keytab "${user}.keytab" \
 --conf spark.network.timeout=10000000 \
 --conf "spark.executor.extraJavaOptions=-Xss30m" \
 --conf "spark.driver.extraJavaOptions=-Xss30m" \
 --conf "yarn.nodemanager.resource.memory-mb=12000" \
 --conf "spark.dynamicAllocation.enabled=true" \
 --conf "spark.sql.catalogImplementation=hive" \
 --conf spark.yarn.executor.memoryOverhead="36g" \
 --queue=${queue} \
 --class=${class} \
 --master yarn \
 --deploy-mode cluster \
 --driver-memory 24G \
 --executor-memory 48G \
 --executor-cores 6 \
 --num-executors 96 \
 --verbose ${jar} \
 $my_param

ret_code=$?
if [[ ${ret_code} -ne 0 ]]; then
    exit ${ret_code}
fi

param_id=$(hive -e "select max(EXEC_ID) from custom_cb_k7m_aux.etl_exec_stts")

curl --insecure -X POST -H 'Content-Type: application/json' ${ctl}/v1/api/statval/m -d '{"loading_id":'${loading_id}',"entity_id": '${entity}',"stat_id": 2,"avalue": ["1"]}'
curl --insecure -X POST -H 'Content-Type: application/json' ${ctl}/v1/api/statval/m -d '{"loading_id":'${loading_id}',"entity_id": 789000439,"stat_id": 223344,"avalue": '${param_id}'}'