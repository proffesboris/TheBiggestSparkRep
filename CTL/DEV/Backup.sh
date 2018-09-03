TIMESTAMP=$(date +%Y%m%d%H%M%S)

if hdfs dfs -test -e $2/snp/smart_src;then
  hdfs dfs -mkdir -p $1/arch_${TIMESTAMP}
  hdfs dfs -cp $2/snp/smart_src $1/arch_${TIMESTAMP}
fi