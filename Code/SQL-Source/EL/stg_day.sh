echo $1 >AAstart$1.txt
nohup hive -v -hiveconf DT1=$1 -f stg_day_acc_rest.sql >result_dayrest_$1.txt 
nohup hive -v -hiveconf DT1=$1 -f stg_day.sql >result_day_$1.txt 
nohup hive -v -hiveconf DT1=$1 -f EL5222.sql >result5222_day_$1.txt 
nohup hive -v -hiveconf DT1=$1 -f EL5211.sql >result5211_$1.txt 
nohup hive -v -hiveconf DT1=$1 -f EL5212_5214.sql >result5212_$1.txt 
nohup hive -v -hiveconf DT1=$1 -f EL5213.sql >result5213_$1.txt 
nohup hive -v -f all_EL_to_Nikita2.sql >result_to_check_$1.txt 
nohup hive -v -f result_table.sql  >result_table_$1.txt 
echo $1 >AZfinish$1.txt