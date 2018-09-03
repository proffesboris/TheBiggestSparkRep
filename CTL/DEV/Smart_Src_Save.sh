TIMESTAMP=$(date +%Y%m%d%H%M%S)

DELETE_TIME=99999999999999

beeline -u "jdbc:hive2://hw2288-02.cloud.dev.df.sbrf.ru:10000/default;principal=hive/_HOST@DEV.DF.SBRF.RU" -f Create_table.hql --hivevar schema=custom_cb_prepproval_mdb --hivevar time=$TIMESTAMP
if [ $(hdfs dfs -ls $1/hist/ | grep -v ^l | wc -l) -gt 25 ] 
then
	for file_name in $(hdfs dfs -stat "%n" $1/hist/*)
	do
		if [ ${file_name##*_} -lt ${DELETE_TIME} ]; then
			DELETE_TIME=${file_name##*_}
		fi
	done 
	hdfs dfs -rm -r $1/hist/smart_src_${DELETE_TIME} 
	beeline -u "jdbc:hive2://hw2288-02.cloud.dev.df.sbrf.ru:10000/default;principal=hive/_HOST@DEV.DF.SBRF.RU" -f Delete_table.hql --hivevar schema=custom_cb_preapproval_mdb_hist --hivevar delete_time=$DELETE_TIME 
fi