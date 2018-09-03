DELETE_TIME=99999999999999

if [ $(hdfs dfs -ls $1/ | grep -v ^l | wc -l) -gt 2 ]
then
	for file_name in $(hdfs dfs -stat "%n" $1/*)
	do
		if [ ${file_name##*_} -lt ${DELETE_TIME} ]; then
			DELETE_TIME=${file_name##*_}
		fi
	done 
	hdfs dfs -rm -r  $1/arch_${DELETE_TIME} 
fi