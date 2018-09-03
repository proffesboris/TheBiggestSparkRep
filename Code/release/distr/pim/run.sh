# use without "publishStatOpt\": {\"stat_id\": ${stat_id}, \"entity_id\": ${entity_id}, \"loading_id\": ${loading_id}, \"avalue\": [\"1\"] }
# if you don't want to publish CTL 'CHANGE' stat
kinit -kt ${USER_NAME}.keytab ${USER_NAME}@DF.SBRF.RU
klist
http_code=$(curl --negotiate -u : -v -s -o out.txt -w '%{http_code}' -H 'Content-Type: application/json' -X POST ${BMES_ADDRESS}/api/executions/${PM_ID} -d "{\"queue\": \"${QUEUE}\", \"userName\": \"${USER_NAME}\", \"inputTables\": \"${INPUT_TABLES}\", \"outputTables\": \"${OUTPUT_TABLES}\"}")
echo "status code=${http_code}"
if [ "$http_code" = "200" ];
then
	execId=`cat out.txt`
	echo "execId = $execId"
	while true
	do
		execInfo=$(curl --negotiate -u : -H 'Content-Type: application/json' -s -X GET ${BMES_ADDRESS}/api/executions/pmExecution/${execId} | grep "status")
		case "$execInfo" in
	  		*NotStarted*)
				echo "execId = $execId is running"
				sleep 5s
				continue
            ;;
	  		*InProgress*)
				echo "execId = $execId is running"
				sleep 5s
				continue
	    	;;
	    	*FailedByAnother*)
				echo "PM ${PM_ID} failed"
				exit 1
			;;
	    	*Failed*)
				echo "PM ${PM_ID} failed"
				exit 1
			;;
	    	*Rejected*)
				echo "PM ${PM_ID} failed"
				exit 1
			;;
			*Success*)
				echo "PM ${PM_ID} succeeded"
				curl --insecure -X POST -H 'Content-Type: application/json' ${ctl}/v1/api/statval/m -d '{"loading_id":'${loading_id}',"entity_id": '${entity}',"stat_id": 2,"avalue": ["1"]}'
				exit 0
			;;
		esac
   done
else
    exit 1
fi
