hdfs dfs -mv $2/snp/counterp $2/hist/
if [ $(hdfs dfs -ls $2/snp/ | grep -v ^l | wc -l) -gt 0 ]
then
    hdfs dfs -rm -r $2/snp/*
fi
hdfs dfs -mv $2/hist/counterp $2/snp/

TIME=00000000000000

if [ $(hdfs dfs -ls $1/ | grep -v ^l | wc -l) -gt 0 ]
then
	for file_name in $(hdfs dfs -stat "%n" $1/*)
	do
		if [ ${file_name##*_} -gt ${TIME} ]; then
			TIME=${file_name##*_}
		fi
	done 
	hdfs dfs -cp $1/arch_${TIME}/* $2/snp/
	hdfs dfs -rm -r $1/arch_${TIME}
fi