TIMESTAMP=$(date +%Y%m%d%H%M | tail -c+3) # Timestamp starting from 18 instead of 2018
# Create Backup directories
hdfs dfs -mkdir -p /data/custom/cb/k7m/bkp/$TIMESTAMP/aux
hdfs dfs -mkdir -p /data/custom/cb/k7m/bkp/$TIMESTAMP/pa
# Move files to backup directories 
hdfs dfs -cp /data/custom/cb/k7m/aux/* /data/custom/cb/k7m/bkp/$TIMESTAMP/aux
hdfs dfs -cp /data/custom/cb/k7m/pa/* /data/custom/cb/k7m/bkp/$TIMESTAMP/pa