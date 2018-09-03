export HADOOP_CONF_DIR=/etc/hive/conf
spark2-submit \
 --principal 'u_dc_s_cb_preapproval_mdb@DEV.DF.SBRF.RU' \
 --keytab 'u_dc_s_cb_preapproval_mdb.keytab' \
 --conf spark.network.timeout=10000000 \
 --conf "spark.executor.extraJavaOptions=-Xss30m" \
 --conf "spark.driver.extraJavaOptions=-Xss30m" \
 --conf "yarn.nodemanager.resource.memory-mb=12000" \
 --conf "spark.dynamicAllolcation.enabled=true" \
 --queue=root.smart_pre_approval \
 --class Basis_KSB_Main \
 --master yarn \
 --deploy-mode cluster \
 --driver-memory 20G \
 --executor-memory 36G \
 --executor-cores 5 \
 --num-executors 50 \
 --verbose Basis_KSB.jar -vvv
