#!/bin/bash
echo "------------------"
echo "Create Directories"
echo "------------------"
hdfs dfs -mkdir -p /data/custom/cb/preapproval_mdb/stg
hdfs dfs -mkdir -p /oozie-app/cb/preapproval_mdb
hdfs dfs -chown -R $TECH_USER:$TECH_USER /oozie-app/cb/preapproval_mdb 
hdfs dfs -chown -R $TECH_USER:$TECH_USER /data/custom/cb/preapproval_mdb/stg