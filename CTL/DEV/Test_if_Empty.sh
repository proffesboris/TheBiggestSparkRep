if [ $(hdfs dfs -du -s  $1/snp/smart_src | cut -d' ' -f1) = 0 ]
then
	exit 1
fi

