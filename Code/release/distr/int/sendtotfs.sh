#!/bin/bash
# copyright Кудрявцев Станислав Андреевич 
# Kudryavtsev.S.A@sberbank.ru 
# Kudryavtsev.S.A@omega.sbrf.ru

HDFS_FOLDER_ARCHIVE="$HDFS_FOLDER/archive/"
echo "----------------------------------"
echo "REMOVE LOCAL FOLDER: $LOCAL_FOLDER"
echo "----------------------------------"
rm -rf $LOCAL_FOLDER

echo "----------------------------------"
echo "CREATE EMPTY LOCAL FOLDER: $LOCAL_FOLDER"
echo "----------------------------------"
mkdir $LOCAL_FOLDER

echo "----------------------------------"
echo "DOWNLOAD FROM HDFS: $HDFS_FOLDER"
echo "----------------------------------"
hdfs dfs -ls $HDFS_FOLDER | grep "^-" | awk '{print $8}' | awk -v folder="$LOCAL_FOLDER" '{system("hdfs dfs -get " $0 " ./" folder)}'

echo "----------------------------------"
echo "DOWNLOAD FROM HDFS: DONE"
echo "----------------------------------"

echo "----------------------------------"
echo "START EXPORT STAGE: $STAGE OF 2"
echo "----------------------------------"
java -cp integration.jar ru.sberbank.sdcb.k7m.core.pack.IntegrationMain $DESTINATION_SYS $KAFKA_BROKER $STAGE
ret_code=$?
if [[ ${ret_code} -ne 0 ]]; then
    exit ${ret_code}
fi
echo "----------------------------------"
echo "END EXPORT STAGE: $STAGE OF 2"
echo "----------------------------------"

if [[ $STAGE = '2' ]]; then
    echo "----------------------------------"
    echo "MOVE $HDFS_FOLDER TO ARCHIVE: $HDFS_FOLDER_ARCHIVE"
    echo "----------------------------------"
    hdfs dfs -ls $HDFS_FOLDER | grep "^-" | awk '{print $8}' | awk -v folder="$HDFS_FOLDER_ARCHIVE" '{system("hdfs dfs -mv " $0 " " folder)}'

    echo "----------------------------------"
    echo "NOTIFY CTL"
    echo "----------------------------------"
    curl --insecure -X POST -H 'Content-Type: application/json' ${ctl}/v1/api/statval/m -d '{"loading_id":'${loading_id}',"entity_id": '${entity}',"stat_id": 2,"avalue": ["1"]}'
fi