set hive.exec.reducers.max=1000;
set mapreduce.map.memory.mb=5240;
set mapreduce.reduce.memory.mb=5240;
set mapred.map.child.java.opts=-Xmx5g;
set mapreduce.map.java.opts=-Xmx5g;
set hive.exec.dynamic.partition=true; 
set hive.exec.dynamic.partition.mode=nonstrict;  
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=10000;

-- Остатки на счетах за дату
insert overwrite table   t_team_k7m_aux_p.k7m_acc_rest PARTITION(oper_dt = '${hiveconf:DT1}')
select 
    c_arc_move,
    C_BALANCE,
    C_BALANCE_LCL
from (
	select 
        r2.collection_id c_arc_move,
        cast(cast(C_START_SUM as double)as decimal(17,2))+direction*cast(cast(C_SUMMA as double)as decimal(17,2)) C_BALANCE,                        
        cast(cast(C_START_SUM_NAT as double)as decimal(17,2)) +direction*cast(cast(C_SUMMA_nat as double)as decimal(17,2)) C_BALANCE_LCL,
        row_number () over (partition by r2.collection_id order by r2.rn desc ) rn2
    from (
            select
               r.collection_id,
               C_START_SUM,
               C_START_SUM_NAT,
               C_SUMMA_nat,
               C_SUMMA,
               case when C_DT = '1' then -1 else 1 end direction ,
               row_number () over (partition by r.collection_id order by c_date desc, id desc ) rn
            from core_internal_eks.z_records r
            where r.c_date < unix_timestamp(cast(date'${hiveconf:DT1}' as timestamp)   )*1000 +24*3600*1000
                and r.c_date >= unix_timestamp(cast(date'2017-01-01' as timestamp))*1000
                and r.ods_opc <> 'D' --отбросить удаленные из DT_010_900002.Z#records
            UNION all
            --Входящие остатки
            select
               collection_id,
               c_balance C_START_SUM,
               c_balance_lcl C_START_SUM_NAT,
               0 C_SUMMA_nat,
               0 C_SUMMA,
               1 direction ,
               0 rn
            FROM t_team_k7m_stg.eks_z_records_in_20161231
            where c_balance <> 0
          ) r2
    where rn<=1
    ) x
where  rn2=1   ;
    
    
-- CREATE TABLE `t_team_k7m_aux_p`.`k7m_acc_rest`  ( 
	 -- `c_arc_move`   	string, 
	 -- `c_balance`    	decimal(17,2), 
	 -- `c_balance_lcl`	decimal(17,2))
 -- PARTITIONED BY (oper_dt STRING) 
 -- STORED AS PARQUET 