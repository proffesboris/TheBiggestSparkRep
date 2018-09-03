
# coding: utf-8

# In[3]:

import os
import sys
spark_home = '/opt/cloudera/parcels/SPARK2_INCLUDE_SPARKR/lib/spark2'
os.environ['SPARK_HOME'] = spark_home
os.environ['PYSPARK_DRIVER_PYTHON'] = 'python'
os.environ['PYSPARK_PYTHON'] = '/opt/cloudera/parcels/PYENV.ZNO20008661/bin/python'
sys.path.insert(0, os.path.join (spark_home,'python'))
sys.path.insert(0, os.path.join (spark_home,'python/lib/py4j-0.10.4-src.zip'))
from pyspark import SparkContext, SparkConf, HiveContext
conf = SparkConf().setAppName('GroupInfluenceIn').    setMaster("yarn-client").    set('spark.local.dir', 'sparktmp').    set('spark.driver.memory','8g').    set('spark.executor.memory','8g').    set('spark.driver.maxResultSize','0').    set('spark.executor.instances', '600').    set('spark.port.maxRetries', '150').    set('spark.executor.cores', '1').    set('spark.rpc.askTimeout', '240s').    set('spark.dynamicAllocation.enabled', 'false').     set('spark.kryoserializer.buffer.max', '2000m') 

sc = SparkContext.getOrCreate(conf=conf)
sqlContext = HiveContext(sc)
import pandas as pd
import pickle
import numpy as np
import time


# # Оглавление 
# 
# 1. [LKJ (RELJ)](#p1)  
#     1.1. [Входные параметры](#p1_1)    
#     1.2. [Разделить экономические критерии](#p1_2)   
#     1.3. [Разделить юридические критерии](#p1_3)   
#     1.4. [Сбор критериев вместе](#p1_4)   
#     1.5. [Проверки](#p1_5) 
# 2. [GroupInfluenceIN](#p2)     
#     2.1. [Срезы ЕГРИП](#p2_1)    
#     2.2. [Разделение на таблицы UnderTheSway и InfluenceOver](#p2_2)     
#     2.3. [Расширенный базис](#p2_3)     
#     2.4. [Расчет выручки](#p2_4)   
#       2.4.1. [Входные параметры](#p2_4_1)     
#       2.4.2. [Выручка Росстат за предыдущий календарный год (или предшествующий ему)](#p2_4_2)     
#       2.4.3. [Выручка за предыдущий календарный год по транзакциям](#p2_4_3)  
#       2.4.4. [Выручка LTM](#p2_4_4)    
#       2.4.5. [Финальная выручка](#p2_4_5)    
#     2.5. [Получение активов из Интегрум](#p2_5)  
#     2.6. [Обогащение расширенного базиса активами и выручкой](#p2_6)  
#     2.7. [Получение PD из витрины PD_Standalone](#p2_7)   
#     2.8. [Фильтрация критериев](#p2_8)    
#       2.8.1. [Фильтрация 5.1.9bgr](#p2_8_1)    
#       2.8.2. [Фильтрация 5.2.2.2_g](#p2_8_2)    
#       2.8.3. [Фильтрация 5.2.2.2](#p2_8_2)    
#     2.9. [Формирование выходной витрины](#p2_9)    
# 3. [Подвал -- справочники со входными параметрами](#p3)   

# # 1. LKJ (RELJ) <a name="p1"></a>

# ## 1.1. Входные параметры  <a name="p1_1"></a>

# In[4]:

schema = 'u_zelinskiy_nr'
target = 'target_to' # префикс таблиц с ИНН из базиса
ul_crits_table = 'u_zelinskiy_nr.ul_new_v3'
el_crits_table = 'u_zelinskiy_nr.k7m_EL_final'
SBRF_INN = '7707083893'
default_up = 1000000
default_down = -1
ul_crits = pickle.load(open('ul_crits.pkl','rb'))  # см подвал
el_crits = pickle.load(open('el_crits.pkl','rb'))  # см подвал
sbl_crits = pickle.load(open('sbl_crits.pkl','rb'))  # см подвал
limitations_up_dict = pickle.load(open('limitations_up.pkl','rb'))  # см подвал
limitations_down_dict = pickle.load(open('limitations_down.pkl','rb'))  # см подвал
deprecated_crits = pickle.load(open('deprecated_crits.pkl','rb'))  # см подвал
crits = ul_crits + el_crits + sbl_crits


# ## 1.2. Разделить эк. критерии <a name="p1_2"></a>

# In[12]:

el_crits_table_names = list(map(lambda x : x.replace('.',''), el_crits)).copy()
for crit, crit_nm in zip(el_crits, el_crits_table_names):
    sql_query = """  
        select from_inn as inn1, to_inn as inn2, quantity, report_dt as dt, probability as link_prob from {}
        where (crit_code = '{}') 
        
    """.format(el_crits_table, crit)
    sqlContext.sql(sql_query).write.format('orc').mode('overwrite').saveAsTable("{}.crit_{}".format(schema, crit_nm))


# ## 1.3. Разделить юр. критерии <a name="p1_3"></a>

# In[15]:

ul_crits_table_names = list(map(lambda x : x.replace('.',''), ul_crits)).copy()
for crit, crit_nm in zip(ul_crits, ul_crits_table_names):
    sql_query = """
        select t_from as inn1, t_to as inn2, quantity, dt,link_prob from {}
        where (crit = '{}') 
    """.format(ul_crits_table, crit)
    sqlContext.sql(sql_query).write.format('orc').mode('overwrite').saveAsTable("{}.crit_{}".format(schema, crit_nm))


# ## 1.4. Сбор критериев вместе <a name="p1_4"></a>

# In[127]:

sql_query = """
select 
    '' as inn1, 
    '' as inn2,     
    '' as quantity,
    '' as dt,
    '' as crit, 
    '' as link_prob 
    from {}.target_all
    where (1 <> 1)
""".format(schema)
empty_sdf =  sqlContext.sql(sql_query)
for crit in crits:
    if crit in deprecated_crits:
        continue
    else:
        sqlContext.sql("refresh table {}.crit_{}".format(schema, crit))
        cond_up = default_up
        try:
            cond_up = limitations_up_dict['{}'.format(crit)]
        except KeyError:
            cond_up = default_up
        cond_down = default_down
        try:
            cond_down = limitations_down_dict['{}'.format(crit)]
        except KeyError:
            cond_down = default_down


        if crit.find('5') > -1:
            sql_query = """
                    select 
                            inn1
                            ,inn2
                            ,quantity
                            ,case when dt = '2016-09-30' then '2016-10-01'
                                  when dt = '2015-03-31' then '2015-04-01'
                                  when dt = '2016-06-30' then '2016-07-01'
                                  when dt = '2015-06-30' then '2015-07-01'
                                  when dt = '2015-09-30' then '2015-10-01'
                                  when dt = '2015-12-31' then '2016-01-01'
                                  when dt = '2016-03-31' then '2016-04-01'
                                  else dt
                            end as dt
                            ,'{}' as crit
                            ,link_prob

                    from {}.crit_{}
                    where ((quantity <= {}) and (quantity >= {}) or quantity is null)
                        and (length(inn1) = 10 or length(inn1) = 12 ) 
                        and (length(inn2) = 10 or length(inn2) = 12 )       
                        and inn1 not like '000%' 
                        and inn2 not like '000%'
                        and inn1 <> '{}'
                        and inn2 <> '{}'

            """.format(crit, schema, (str(crit).replace('.','')), cond_up, cond_down, SBRF_INN, SBRF_INN)
        else:
            sql_query = """
                    select 
                            inn1
                            ,inn2
                            ,quantity
                            ,case when dt = '2016-09-30' then '2016-10-01'
                                  when dt = '2015-03-31' then '2015-04-01'
                                  when dt = '2016-06-30' then '2016-07-01'
                                  when dt = '2015-06-30' then '2015-07-01'
                                  when dt = '2015-09-30' then '2015-10-01'
                                  when dt = '2015-12-31' then '2016-01-01'
                                  when dt = '2016-03-31' then '2016-04-01'
                                  else dt
                            end as dt
                            ,'{}' as crit
                            ,'' as link_prob
                    from {}.crit_{}
                    where ((quantity <= {}) and (quantity >= {}) or quantity is null)
                        and (length(inn1) = 10 or length(inn1) = 12) 
                        and (length(inn2) = 10 or length(inn2) = 12) 
                        and inn1 not like '000%' 
                        and inn2 not like '000%'
                        and inn1 <> '{}'
                        and inn2 <> '{}'

            """.format(crit, schema, crit, cond_up, cond_down, SBRF_INN, SBRF_INN)
        empty_sdf = empty_sdf.union(sqlContext.sql(sql_query))
empty_sdf.write.format('orc').mode('overwrite').saveAsTable("{}.all_crit".format(schema))


# ## 1.5. Проверки <a name="p1_5"></a>

# In[128]:

crits4 = sqlContext.sql("select crit, count(*) as cnt from {}.all_crit group by crit order by cnt desc".format(schema)).toPandas()
crits4


# In[129]:

sqlContext.sql("select distinct dt from u_zelinskiy_nr.all_crit").show()


# In[130]:

sqlContext.sql("select dt, count(distinct inn1), count(distinct inn2) from u_zelinskiy_nr.all_crit group by dt").show()


# In[131]:

sqlContext.sql("select distinct crit from u_zelinskiy_nr.all_crit group by dt, inn1, inn2, crit having count(quantity)  > 1").show()


# # 2. GroupInfluenceIN <a name="p2"></a>

# ## 2.1. Срезы ЕГРИП <a name="p2_1"></a>

# In[135]:

date_mass_cond = ['2016-10-01', '2015-04-01', '2016-07-01', '2015-07-01','2015-10-01', '2016-01-01', '2016-04-01']
egrip = sqlContext.sql("select '' as ip_inn, '' as dt from {}.target_all where (1 <> 1)".format(schema))
for date_cond in date_mass_cond:
    print('{}'.format(date_cond))
    sql_query = """
    select a.ip_inn, '{}' as dt 
    
    from
            (
                select  
                        row_number() over (partition by ip_org_id order by valid_from desc) as rn,
                        ip_inn,
                        ip_active_flg
                        
                    from core_internal_integrum.ip_organization_egrip
                    where 
                        (valid_from  < '{}') and 
                        (valid_to  > '{}')
                ) a
                where a.rn = 1 and a.ip_active_flg = 1
    """.format(date_cond, date_cond, date_cond)
    egrip = egrip.union(sqlContext.sql(sql_query))
egrip.createOrReplaceTempView('egrip')
egrip.count()


# ## 2.2. Разделение на таблицы UnderTheSway и InfluenceOver <a name="p2_2"></a>

# In[136]:

sql_query = """
                select a.inn1, a.inn2, a.quantity, a.dt, a.crit, a.link_prob
                from {}.all_crit a
                inner join {}.target_all b
                on a.inn2 = b.inn and a.dt = b.od
                
                where (
                    (length(a.inn1) = 10) or 
                    ((length(a.inn1) = 12) and (a.inn1 in (select ip_inn from egrip where dt = a.dt)))

                    ) 
                    and
                    (
                    (length(a.inn2) = 10) or 
                    ((length(a.inn2) = 12) and (a.inn2 in (select ip_inn from egrip where dt = a.dt)))
                
                    )
            """.format(schema, schema)
sqlContext.sql(sql_query).write.format('orc').mode('overwrite').saveAsTable("{}.UnderTheSway".format(schema))


# In[137]:

sql_query = """
                select a.inn1, a.inn2, a.quantity, a.dt, a.crit, a.link_prob
                from {}.all_crit a
                inner join {}.target_all b
                on a.inn1 = b.inn and a.dt = b.od
                
               where (
                    (length(a.inn1) = 10) or 
                    ((length(a.inn1) = 12) and (a.inn1 in (select ip_inn from egrip where dt = a.dt)))

                    ) 
                    and
                    (
                    (length(a.inn2) = 10) or 
                    ((length(a.inn2) = 12) and (a.inn2 in (select ip_inn from egrip where dt = a.dt)))
                
                    )
                   
                
            """.format(schema, schema)
sqlContext.sql(sql_query).write.format('orc').mode('overwrite').saveAsTable("{}.InfluenceOver".format(schema))


# ## 2.3. Расширенный базис -- BasisExtended <a name="p2_3"></a>

# In[138]:

# получаем список ИНН, по которому на операции BasisEKSIn2 будут выгружаться транзакции
sql_query = "select distinct inn1 as inn, dt from {}.UnderTheSway".format(schema)
empty_sdf = sqlContext.sql(sql_query)
sql_query = "select distinct inn2 as inn, dt from {}.InfluenceOver".format(schema)
empty_sdf = empty_sdf.union(sqlContext.sql(sql_query))
sql_query = "select distinct inn, od as dt from {}.target_all".format(schema)
empty_sdf = empty_sdf.union(sqlContext.sql(sql_query))
empty_sdf.createOrReplaceTempView("inn_for_ext_basis")
df2 = sqlContext.sql("select distinct inn, dt from inn_for_ext_basis")
df2.write.format('orc').mode('overwrite').saveAsTable("{}.inn".format(schema))
df2.write.format('orc').mode('overwrite').saveAsTable("t_corp_risks.znr_inn_for_ratings")


# In[139]:

# sql_query = """
# select * from t_corp_risks.znr_inn_for_ratings
# """
# sqlContext.sql(sql_query).write.format('orc').mode('overwrite').saveAsTable("{}.inn_backup".format(schema))


# In[140]:

sqlContext.sql("select dt, count(inn) from t_corp_risks.znr_inn_for_ratings where length(inn) = 10 group by dt").show()


# In[141]:

sqlContext.sql("select distinct inn from t_corp_risks.znr_inn_for_ratings ").count()


# In[142]:

sqlContext.sql("select distinct inn from t_corp_risks.znr_inn_for_ratings where length(inn) = 10").count()


# ## Разметка транзакций по этим ЮЛ происходит на операции TRClass, 
# ## их выгрузка осуществляется на операции BasisEKSIn2

# ## 2.4. Расчет выручки (по витрине размеченных транзакций) <a name="p2_4"></a>

# ### 2.4.1. Входные параметры <a name="p2_4_1"></a>

# In[12]:

schema = 'u_zelinskiy_nr'
revenue_down_limit = 1000000 # компании с выручкой менее 1 млн рублей выглядят странно
REVENUE_CUT_OFF_LOW = 0.1 # если по транзакциям выручка 0.1 от Росстатовской, то выручке по транзакциям не верим
# например, через нас клиент проводит меньшую часть транзакций
REVENUE_CUT_OFF_HIGH = 3 # если по транзакциям выручка в 100 раз больше Росстатовской, то кто-то из них врет
# например, управляющая компания

CONST_REVENUE_UP_LIMIT = 1.2 # Верхнее ограничение на нормировочный коэффициент по транзакциям
CONST_REVENUE_DOWN_LIMIT = 0.8 # Нижнее ограничение на нормировочный коэффициент по транзакциям

# размеченные транзакции по расширенному базису здесь:
# t_corp_risks.innimport_z260318_6_out


# ### 2.4.2. Выручка Росстат за предыдущий календарный год (или предшествующий ему) <a name="p2_4_2"></a>

# In[144]:

time_start = time.time()
date_mass_to = ['2016_09_30', '2015_03_31', '2016_06_30', '2015_06_30','2015_09_30', '2015_12_31', '2016_03_31']
date_mass_cond_to = ['2016-10-01', '2015-04-01', '2016-07-01', '2015-07-01','2015-10-01', '2016-01-01', '2016-04-01']
date_mass_cond_from = ['2015-10-01', '2014-04-01', '2015-07-01', '2014-07-01','2014-10-01', '2015-01-01', '2015-04-01']
revRO_df = sqlContext.sql("select '' as inn, '' as dt, '' as revenue_rosstat from {}.inn where (1 <> 1)".format(schema))
for date_to, date_cond_from, date_cond_to in zip(date_mass_to, date_mass_cond_from, date_mass_cond_to):
    print('{}_{}_{}'.format(date_to, date_cond_from, date_cond_to))
        #Выручка этих компаний по Росстату за прошлый год
    year_cond = str(int(date_cond_to[0:4]) - 1) + ' год'
    year_cond_beflast = str(int(date_cond_to[0:4]) - 2) + ' год'
    print(year_cond)
    sql_query = """
    select 
            dd.inn
             , '{}' as dt
             , greatest(max(dd.rev_last), max(dd.rev_beflast)) as revenue_rosstat
    from (
        select
                c.inn
                , case when period_nm = '{}' and revenue_rosstat is null then 0
                       when period_nm = '{}' and revenue_rosstat is not null then revenue_rosstat
                    else 0 end as rev_beflast
                , case when period_nm = '{}' and revenue_rosstat is null then 0
                       when period_nm = '{}' and revenue_rosstat is not null then revenue_rosstat
                    else 0 end as rev_last
        from(
                select 
                            b.ul_inn as inn    
                            , period_nm
                            , max(cast(r.fs_value as decimal(22,5))) as revenue_rosstat


                from
                    (
                        select ul_org_id, fs_value, period_nm
                        from
                        core_internal_integrum.financial_statement_rosstat 
                        where  ( 
                                   (fs_line_num = '2110') 
                                    and (period_nm in ('{}', '{}'))
                                    and (fs_line_cd = 'P21103')
                                )
                    ) r

                left join 
                (
                    select a.ul_org_id, a.ul_inn
                    from (
                            select  
                                row_number() over (partition by ul_org_id order by valid_from desc) as rn,
                                * 
                                from core_internal_integrum.ul_organization_rosstat
                                where 
                                    valid_from < '{}'
                               --  and valid_to > '{}'


                            ) a
                            where a.rn = 1


                ) b

                on r.ul_org_id = b.ul_org_id
                left join 
                            (
                                select inn from {}.inn where dt = '{}'
                            ) ef
                on b.ul_inn =  ef.inn          
                where ef.inn is not null
                group by b.ul_inn, period_nm
        ) c
    ) dd
    group by dd.inn
    """.format(date_cond_to, year_cond,  
               year_cond_beflast, year_cond_beflast,
               year_cond, year_cond, 
               year_cond_beflast, date_cond_to, 
               date_cond_to, schema, 
               date_cond_to)
    revRO_df = revRO_df.union(sqlContext.sql(sql_query))
revRO_df.createOrReplaceTempView('revRO_df')

sql_query = """
select 
        inn
        , dt
        , case when revenue_rosstat < {} then 0 else revenue_rosstat end as revenue_rosstat
 from revRO_df
""".format(revenue_down_limit)
revRO_df = sqlContext.sql(sql_query) 
revRO_df.write.format('orc').mode('overwrite').saveAsTable('{}.revRO_test'.format(schema))
revRO_df = sqlContext.sql('select * from {}.revRO_test'.format(schema))
revRO_df.createOrReplaceTempView('revRO_df')
print(time.time() - time_start)


# In[145]:

sql_query = """
select * from {}.revRO_test
""".format(schema)
sqlContext.sql(sql_query).count()


# In[146]:

sql_query = """
select * from {}.revRO_test
where revenue_rosstat > 1
""".format(schema)
sqlContext.sql(sql_query).count()


# In[174]:

201941 /238414


# ### 2.4.3. Выручка за предыдущий календарный год по транзакциям <a name="p2_4_3"></a>

# In[148]:

time_start = time.time()
date_mass_to = ['2016_09_30', '2015_03_31', '2016_06_30', '2015_06_30','2015_09_30', '2015_12_31', '2016_03_31']
date_mass_cond_to = ['2016-10-01', '2015-04-01', '2016-07-01', '2015-07-01','2015-10-01', '2016-01-01', '2016-04-01']
date_mass_cond_from = ['2015-10-01', '2014-04-01', '2015-07-01', '2014-07-01','2014-10-01', '2015-01-01', '2015-04-01']
revLY_df = sqlContext.sql("select '' as inn, '' as dt, '' as revenue_LY from {}.inn where (1 <> 1)".format(schema))
for date_to, date_cond_from, date_cond_to in zip(date_mass_to, date_mass_cond_from, date_mass_cond_to):
    print('{}_{}_{}'.format(date_to, date_cond_from, date_cond_to))
    #Выручка этих компаний по транзакциям за прошлый год
    date_cond_LY_from = str(int(date_cond_to[0:4]) - 1) + '-01-01'
    date_cond_LY_to = str(int(date_cond_to[0:4]) - 1) + '-12-31'
    sql_query = """
    select  
                c_kl_kt_2_inn as inn,
                '{}' as dt,
                sum(cast(c_sum_nt as decimal(23,5))) as revenue_LY

    from t_corp_risks.innimport_z260318_6_out v
    
    where v.PREDICTED_VALUE in (
                                'эквайринг',
                                'инкассация',
                                'оплата по договору',
                                'лизинг',
                                'аренда',
                                'дивиденды'
                                )
            and         
            (
                c_kl_kt_2_inn in 
                    (

                        select inn from from {}.inn where dt = '{}'
                    )
            )
            and (v.c_date_prov <= '{}')   
            and  (v.c_date_prov >= '{}')
           

    group by c_kl_kt_2_inn

    """.format(date_cond_to, schema, date_cond_to, date_cond_LY_to, date_cond_LY_from)
    revLY_df = revLY_df.union(sqlContext.sql(sql_query))
revLY_df.createOrReplaceTempView('revLY_df')    

sql_query = """
select 
        a.inn
        , a.dt
        , case when (b.revenue_rosstat < 1) or (b.revenue_rosstat is null) then a.revenue_LY
               when  a.revenue_LY/b.revenue_rosstat > {} then 0
               when  a.revenue_LY/b.revenue_rosstat < {} then 0 
               else a.revenue_LY
               end as revenue_LY

     from revLY_df a
     left join revRO_df b
     on a.inn = b.inn and a.dt = b.dt

""".format(REVENUE_CUT_OFF_HIGH, REVENUE_CUT_OFF_LOW)

revLY_df = sqlContext.sql(sql_query)
revLY_df.write.format('orc').mode('overwrite').saveAsTable('{}.revLY_test'.format(schema))
revLY_df = sqlContext.sql('select * from {}.revLY_test'.format(schema))
revLY_df.createOrReplaceTempView('revLY_df')    
print(time.time() - time_start)


# In[149]:

import matplotlib.pyplot as plt 
sql_query = """
     select a.revenue_LY/b.revenue_rosstat as quantity 
     from {}.revLY_test a
     full join {}.revRO_test b
     on a.inn = b.inn and a.dt = b.dt
     where (revenue_rosstat > 0) and (revenue_LY > 0)
""".format(schema, schema)

qdf = sqlContext.sql(sql_query).toPandas()
qdf = qdf.astype(np.float)
qdf.hist(column = 'quantity', bins = 20)
plt.show()


# In[150]:

sql_query = """
select * from {}.revLY_test
where revenue_LY > 1
""".format(schema)
sqlContext.sql(sql_query).count()


# In[151]:

sql_query = """
select * from {}.revLY_test
""".format(schema)
sqlContext.sql(sql_query).count()


# In[173]:

171133 / 221746


# ### 2.4.4. Выручка LTM <a name="p2_4_4"></a>

# In[153]:

time_start = time.time()
# t_corp_risks.yia_kras_010217 -- раскрашенные транзакции по расширенному базису
date_mass_to = ['2016_09_30', '2015_03_31', '2016_06_30', '2015_06_30','2015_09_30', '2015_12_31', '2016_03_31']
date_mass_cond_to = ['2016-10-01', '2015-04-01', '2016-07-01', '2015-07-01','2015-10-01', '2016-01-01', '2016-04-01']
date_mass_cond_from = ['2015-10-01', '2014-04-01', '2015-07-01', '2014-07-01','2014-10-01', '2015-01-01', '2015-04-01']
revLTM_df = sqlContext.sql("select '' as inn, '' as dt, '' as revenue_L12M from {}.inn where (1 <> 1)".format(schema))
for date_to, date_cond_from, date_cond_to in zip(date_mass_to, date_mass_cond_from, date_mass_cond_to):
    print('{}_{}_{}'.format(date_to, date_cond_from, date_cond_to))
 # Выручка L12M компаний 
    sql_query = """
        select  
                c_kl_kt_2_inn as inn,    
                '{}' as dt,
                sum(cast(c_sum_nt as decimal(23,5))) as revenue_L12M

        from t_corp_risks.innimport_z260318_6_out 
        where PREDICTED_VALUE in (
                                    'эквайринг',
                                    'инкассация',
                                    'оплата по договору',
                                    'лизинг',
                                    'аренда',
                                    'дивиденды'
                                    )
                and         
                (
                      c_kl_kt_2_inn in 
                        (
                            select inn from {}.inn where dt = '{}'
                        ) 
                )
                and (c_date_prov >= '{}') 
                and (c_date_prov < '{}')
        group by c_kl_kt_2_inn

    """.format(date_cond_to, schema, date_cond_to, date_cond_from, date_cond_to)
    revLTM_df = revLTM_df.union(sqlContext.sql(sql_query))
revLTM_df.createOrReplaceTempView('revLTM_df')    
revLTM_df.write.format('orc').mode('overwrite').saveAsTable('{}.revLTM_test'.format(schema))
revLTM_df = sqlContext.sql('select * from {}.revLTM_test'.format(schema))
revLTM_df.createOrReplaceTempView('revLTM_df')    
print(time.time() - time_start)


# In[154]:

sql_query = """
     select a.revenue_L12M/b.revenue_LY as quantity 
     from {}.revLTM_test a
     inner join {}.revLY_test b
     on a.inn = b.inn and a.dt = b.dt
     where (revenue_L12M > 1) and (revenue_LY > 1)
""".format(schema, schema)

qdf = sqlContext.sql(sql_query).toPandas()
qdf = qdf.astype(np.float)
qdf[qdf['quantity'] < 2].hist(column = 'quantity', bins = 20)
plt.show()


# ### 2.4.5. Финальная выручка <a name="p2_4_5"></a> 

# In[155]:

revLTM_df = sqlContext.sql('select * from {}.revLTM_test'.format(schema))
revLTM_df.createOrReplaceTempView('revLTM_df')    
revLY_df = sqlContext.sql('select * from {}.revLY_test'.format(schema))
revLY_df.createOrReplaceTempView('revLY_df') 
revRO_df = sqlContext.sql('select * from {}.revRO_test'.format(schema))
revRO_df.createOrReplaceTempView('revRO_df')


# In[156]:

inn_df = sqlContext.sql("select inn, dt from {}.inn".format(schema)).cache()
inn_df.count()
print('inn_df cached')
jnd_revLTM_df = revLTM_df.join(inn_df, on = ['inn', 'dt'], how = 'right')
print('1st join')
jnd_revLTM_RO_df = jnd_revLTM_df.join(revRO_df, on = ['inn', 'dt'], how = 'left')
print('2nd join')
jnd_revLTM_RO_LY_df = jnd_revLTM_RO_df.join(revLY_df, on = ['inn', 'dt'], how = 'left')
print('3rd join')
jnd_revLTM_RO_LY_df.createOrReplaceTempView("jnd_revLTM_RO_LY_df")


# In[157]:

sql_query = """
select 
        inn
        , dt
        , coalesce(revenue_L12M, 0) as revenue_L12M
        , coalesce(revenue_rosstat, 0) as revenue_rosstat
        , coalesce(revenue_LY, 0) as  revenue_LY
        , case when coalesce(revenue_LY, 0) > 1 then coalesce(coalesce(revenue_L12M, 0) / coalesce(revenue_LY, 0) , 0) 
            else -1
            end as alpha
        
        
from jnd_revLTM_RO_LY_df
"""
sqlContext.sql(sql_query).write.format('orc').mode('overwrite').saveAsTable('{}.jnd_revLTM_RO_LY_df_test'.format(schema))


# In[158]:

time_start = time.time()
sql_query = """
select 
        inn
        , dt
        , revenue_L12M
        , revenue_rosstat
        , revenue_LY
        , case when revenue_rosstat < 1 and  revenue_L12M > {} then revenue_L12M -- больше revenue_down_limit
               when revenue_rosstat < 1 and  revenue_L12M <= {} then 0 -- меньше revenue_down_limit
               when revenue_rosstat > 1 and  (alpha < {}) and (alpha > {}) then alpha * revenue_rosstat -- между 0.8 и 1.2
               when revenue_rosstat > 1 and  (alpha >= {}) and alpha <= {} then {} * revenue_rosstat --CONST_REVENUE_UP_LIMIT       
               when revenue_rosstat > 1 and  (alpha <= {}) and alpha >= {} then {} * revenue_rosstat --CONST_REVENUE_DOWN_LIMIT
               when revenue_rosstat > 1 and   alpha < {}  then revenue_rosstat 
               when revenue_rosstat > 1 and   alpha > {}  then revenue_rosstat 
               else -99999999
               end as revenue_L12M_true
        
        
 from {}.jnd_revLTM_RO_LY_df_test

""".format(revenue_down_limit, 
           revenue_down_limit,
           CONST_REVENUE_UP_LIMIT, 
           CONST_REVENUE_DOWN_LIMIT,
           CONST_REVENUE_UP_LIMIT, REVENUE_CUT_OFF_HIGH, CONST_REVENUE_UP_LIMIT,
           CONST_REVENUE_DOWN_LIMIT, REVENUE_CUT_OFF_LOW, CONST_REVENUE_DOWN_LIMIT,
           REVENUE_CUT_OFF_LOW,
           REVENUE_CUT_OFF_HIGH,
           schema)

sqlContext.sql(sql_query).write.format('orc').mode('overwrite').saveAsTable("{}.Revenue_Extended_test".format(schema))
print((time.time() - time_start) / 60.0)


# In[159]:

sql_query = """
     select revenue_rosstat/revenue_L12M_true as quantity 
     from {}.Revenue_Extended_test
""".format(schema)

qdf = sqlContext.sql(sql_query).toPandas()
qdf = qdf.astype(np.float)
qdf.hist(column = 'quantity', bins = 20)
plt.show()


# In[160]:

import numpy as np
sql_query = """
     select a.revenue_L12M_true
     from {}.Revenue_Extended_test a
     inner join {}.target_all b
     on a.inn = b.inn and a.dt = b.od
""".format(schema, schema)

qdf = sqlContext.sql(sql_query).toPandas()
qdf = qdf.astype(np.float)

qdf['logrev'] = qdf['revenue_L12M_true'].map(lambda x: np.log10(x + 1))
qdf.hist(column = 'logrev', bins = 20)
ax = plt.gca()
ax.set_title('обучающая выборка')
plt.show()


# In[161]:

sql_query = """
     select a.revenue_L12M_true
     from {}.Revenue_Extended_test a

""".format(schema, schema)

qdf = sqlContext.sql(sql_query).toPandas()
qdf = qdf.astype(np.float)

qdf['logrev'] = qdf['revenue_L12M_true'].map(lambda x: np.log10(x + 1))
qdf.hist(column = 'logrev', bins = 20)
ax = plt.gca()
ax.set_title('их соседи')
plt.show()


# ## 2.5. Получение Активов из Интегрум  <a name="p2_5"></a> 

# In[162]:

time_start = time.time()
date_mass_to = ['2016_09_30', '2015_03_31', '2016_06_30', '2015_06_30','2015_09_30', '2015_12_31', '2016_03_31']
date_mass_cond_to = ['2016-10-01', '2015-04-01', '2016-07-01', '2015-07-01','2015-10-01', '2016-01-01', '2016-04-01']
date_mass_cond_from = ['2015-10-01', '2014-04-01', '2015-07-01', '2014-07-01','2014-10-01', '2015-01-01', '2015-04-01']
assRO_df = sqlContext.sql("select '' as inn, '' as dt, '' as assets_rosstat from {}.inn where (1 <> 1)".format(schema))
for date_to, date_cond_from, date_cond_to in zip(date_mass_to, date_mass_cond_from, date_mass_cond_to):
    print('{}_{}_{}'.format(date_to, date_cond_from, date_cond_to))
    year_cond = str(int(date_cond_to[0:4]) - 1) + ' год'
    year_cond_beflast = str(int(date_cond_to[0:4]) - 2) + ' год'
    print(year_cond)
    sql_query = """
    select 
            dd.inn
             , '{}' as dt
             , greatest(max(dd.ass_last), max(dd.ass_beflast)) as assets_rosstat
    from (
        select
                c.inn
                , case when period_nm = '{}' and assets_rosstat is null then 0
                       when period_nm = '{}' and assets_rosstat is not null then assets_rosstat
                    else 0 end as ass_beflast
                , case when period_nm = '{}' and assets_rosstat is null then 0
                       when period_nm = '{}' and assets_rosstat is not null then assets_rosstat
                    else 0 end as ass_last
        from(
                select 
                            b.ul_inn as inn    
                            , period_nm
                            , max(cast(r.fs_value as decimal(22,5))) as assets_rosstat


                from
                    (
                        select ul_org_id, fs_value, period_nm
                        from
                        core_internal_integrum.financial_statement_rosstat 
                        where  ( 
                                   (fs_line_num = '1600') 
                                    and (period_nm in ('{}', '{}'))
                                    and (fs_line_cd = 'P16003')
                                )
                    ) r

                left join 
                (
                    select a.ul_org_id, a.ul_inn
                    from (
                            select  
                                row_number() over (partition by ul_org_id order by valid_from desc) as rn,
                                * 
                                from core_internal_integrum.ul_organization_rosstat
                                where 
                                    valid_from < '{}'
                               --  and valid_to > '{}'


                            ) a
                            where a.rn = 1


                ) b

                on r.ul_org_id = b.ul_org_id
                left join 
                            (
                                select inn from {}.inn where dt = '{}'
                            ) ef
                on b.ul_inn =  ef.inn          
                where ef.inn is not null
                group by b.ul_inn, period_nm
        ) c
    ) dd
    group by dd.inn
    """.format(date_cond_to, year_cond,  
               year_cond_beflast, year_cond_beflast,
               year_cond, year_cond, 
               year_cond_beflast, date_cond_to, 
               date_cond_to, schema, 
               date_cond_to)
    assRO_df = assRO_df.union(sqlContext.sql(sql_query))
assRO_df.createOrReplaceTempView('assRO_df')

sql_query = """
select 
        inn
        , dt
        , case when assets_rosstat < {} then 0 else assets_rosstat end as assets_rosstat
 from assRO_df
""".format(revenue_down_limit)
assets_df = sqlContext.sql(sql_query) 
assets_df.write.format('orc').mode('overwrite').saveAsTable('{}.assetsRO_test'.format(schema))
assets_df = sqlContext.sql('select * from {}.assetsRO_test'.format(schema))
assets_df.createOrReplaceTempView('assets_df')
print(time.time() - time_start)


# In[163]:

inn_df = sqlContext.sql("select inn, dt from {}.inn".format(schema)).cache()
inn_df.count()
print('inn_df cached')


# In[164]:

time_start = time.time()
assets_df = sqlContext.sql('select * from {}.assetsRO_test'.format(schema))
assets_df = assets_df.join(inn_df, on = ['inn', 'dt'], how = 'right')
assets_df.write.format('orc').mode('overwrite').saveAsTable("{}.assets_true".format(schema))
print(time.time() - time_start)


# ## 2.6. Обогащение расширенного базиса активами и выручкой <a name="p2_6"></a> 

# In[165]:

sql_query = """
select 
        a.inn
        ,a.dt
        ,b.revenue_L12M_true
        ,c.assets_rosstat

from {}.inn a
left join {}.Revenue_Extended_test b
on (a.inn = b.inn) and (a.dt = b.dt)
left join {}.assets_true  c
on (a.inn = c.inn) and (a.dt = c.dt)
""".format(schema, schema, schema)
sqlContext.sql(sql_query).write.format('orc').mode('overwrite').saveAsTable("{}.rich_inn".format(schema))


# In[166]:

sqlContext.sql("select dt, count(inn) from  {}.rich_inn group by dt".format(schema)).show()


# #### Формирование финальной таблицы UnderTheSway

# In[167]:

sql_query = """
    select
            a.inn1
            ,a.inn2
            ,a.dt
            ,a.quantity
            ,a.crit
            ,a.link_prob
            ,b.assets_rosstat as assets1
            ,b.revenue_L12M_true as revenue1
            ,case when c.assets_rosstat < 1 then 0 else b.assets_rosstat / c.assets_rosstat end as assets1_norm
            ,case when c.revenue_L12M_true < 1 then 0 else b.revenue_L12M_true / c.revenue_L12M_true end as revenue1_norm
            ,c.assets_rosstat as assets2
            ,c.revenue_L12M_true as revenue2
            ,1 as revenue2_norm
            ,1 as assets2_norm

        from {}.UnderTheSway a
        left join {}.rich_inn b
        on (a.inn1 = b.inn) and (a.dt = b.dt)
        left join {}.rich_inn c
        on (a.inn2 = c.inn) and (a.dt = c.dt)
""".format(schema, schema, schema)
sqlContext.sql(sql_query).write.format('orc').mode('overwrite').saveAsTable("{}.UnderTheSway_final".format(schema))


# #### Формирование финальной таблицы InfluenceOver

# In[168]:

sql_query = """
    select
            a.inn1
            ,a.inn2
            ,a.dt
            ,a.quantity
            ,a.crit
            ,a.link_prob
            ,b.assets_rosstat as assets1
            ,b.revenue_L12M_true as revenue1
            ,1 as revenue1_norm
            ,1 as assets1_norm
            ,case when c.assets_rosstat < 1 then 0 else b.assets_rosstat / c.assets_rosstat end as assets2_norm
            ,case when c.revenue_L12M_true < 1 then 0 else b.revenue_L12M_true / c.revenue_L12M_true end as revenue2_norm
            ,c.assets_rosstat as assets2
            ,c.revenue_L12M_true as revenue2
            
        from {}.InfluenceOver a
        left join {}.rich_inn b
        on (a.inn1 = b.inn) and (a.dt = b.dt)
        left join {}.rich_inn c
        on (a.inn2 = c.inn) and (a.dt = c.dt)
""".format(schema, schema, schema)
sqlContext.sql(sql_query).write.format('orc').mode('overwrite').saveAsTable("{}.InfluenceOver_final".format(schema))


# ## 2.7. Получение PD из витрины PD_Standalone <a name="p2_7"></a> 

# In[169]:

sql_query = """
select *
from t_corp_risks.yia_nz_predicted010418_cor_with_ext
where coalesce(pd_ttc_final, PD_EXT_TTC) is not null 
"""
sqlContext.sql(sql_query).count()


# In[170]:

sql_query = """
select 
        a.*
        , coalesce(b.pd_ttc_final, b.PD_EXT_TTC) as inn1pd
        , coalesce(d.pd_ttc_final, d.PD_EXT_TTC) as inn2pd
        
from {}.InfluenceOver_final a
left join t_corp_risks.yia_nz_predicted010418_cor_with_ext b
on (a.inn1 = b.inn) and (a.dt = b.od)


left join t_corp_risks.yia_nz_predicted010418_cor_with_ext d
on (a.inn2 = d.inn) and (a.dt = d.od)

""".format(schema)
sqlContext.sql(sql_query).write.format('orc').mode('overwrite').saveAsTable("{}.InfluenceOver_final_rat".format(schema))


# In[171]:

sql_query = """
select 
        a.* 
        , coalesce(b.pd_ttc_final, b.PD_EXT_TTC) as inn1pd
        , coalesce(d.pd_ttc_final, d.PD_EXT_TTC) as inn2pd
        
from {}.UnderTheSway_final a
left join t_corp_risks.yia_nz_predicted010418_cor_with_ext b
on (a.inn1 = b.inn) and (a.dt = b.od)

left join t_corp_risks.yia_nz_predicted010418_cor_with_ext d
on (a.inn2 = d.inn) and (a.dt = d.od)

""".format(schema)
sqlContext.sql(sql_query).write.format('orc').mode('overwrite').saveAsTable("{}.UnderTheSway_final_rat".format(schema))


# ## 2.8. Фильтрация критериев <a name="p2_8"></a> 

# ### Входные параметры

# In[15]:

limit519bgr = 200 # не более 200 связанных ЮЛ на один ИНН для критерия 5.1.9bgr
TOP_5222g_COUNT = 9 # не более девяти связей на один ИНН по критерию 5.2.2.2_g
TOP_5222_COUNT = 3 # не более трех связей на один ИНН по критерию 5.2.2.2_g

MIN_SENDLOANS = 0.02
MIN_RECEIVE_LOANS = 0.01


# ### 2.8.1. Фильтрация 5.1.9bgr <a name="p2_8_1"></a>  

# In[6]:

sql_query = """
select inn2, crit, dt, count(inn1) as link_cnt_519bgr
from {}.UnderTheSway_final_rat
where crit = '5.1.9bgr'
group by inn2, crit, dt
""".format(schema)
bgr_counted_utsw = sqlContext.sql(sql_query)
bgr_counted_utsw.createOrReplaceTempView('bgr_counted_utsw')

sql_query = """
select inn1, crit, dt, count(inn2) as link_cnt_519bgr
from {}.InfluenceOver_final_rat
where crit = '5.1.9bgr'
group by inn1, crit, dt
""".format(schema)
bgr_counted_inov = sqlContext.sql(sql_query)
bgr_counted_inov.createOrReplaceTempView('bgr_counted_inov')


# In[7]:

sql_query = """
select d.*
from 
(
    select a.*, link_cnt_519bgr
    from {}.UnderTheSway_final_rat a
    left join bgr_counted_utsw b
    on a.inn2 = b.inn2 and a.dt = b.dt and a.crit = b.crit
) d
where (crit not in ('5.1.9bgr')) or ((crit in ('5.1.9bgr')) and (link_cnt_519bgr < {}))
""".format(schema, limit519bgr)
sqlContext.sql(sql_query).write.format('orc').mode('overwrite').saveAsTable('{}.UnderTheSway_clean'.format(schema))
sql_query = """
select  d.*    
from 
(
    select a.*, link_cnt_519bgr
    from {}.InfluenceOver_final_rat a
    left join bgr_counted_inov b
    on a.inn1 = b.inn1 and a.dt = b.dt and a.crit = b.crit
) d
where (crit not in ('5.1.9bgr')) or ((crit in ('5.1.9bgr')) and (link_cnt_519bgr < {}))
""".format(schema, limit519bgr)
sqlContext.sql(sql_query).write.format('orc').mode('overwrite').saveAsTable('{}.InfluenceOver_clean'.format(schema))


# ###  2.8.2. Фильтрация 5.2.2.2_g <a name="p2_8_2"></a>  

# In[8]:

sql_query = """
select d.*
from
(
    select row_number() over (partition by inn2, dt order by quantity desc) as rn
           , inn2
           , dt
           , inn1
           , quantity
           , crit
    from {}.UnderTheSway_clean
    where crit = '5.2.2.2_g'
) d
""".format(schema)
cr5222g_utsw = sqlContext.sql(sql_query)
cr5222g_utsw.createOrReplaceTempView('cr5222g_utsw')  
sql_query = """
select d.*
from
(
    select row_number() over (partition by inn1, dt order by quantity desc) as rn
           , inn1
           , dt
           , inn2
           , quantity
           , crit
    from {}.InfluenceOver_clean
    where crit = '5.2.2.2_g'
) d
""".format(schema)
cr5222g_inov = sqlContext.sql(sql_query)
cr5222g_inov.createOrReplaceTempView('cr5222g_inov') 


# In[9]:

sql_query = """
select d.*
from 
(
    select a.*, b.rn
    from {}.UnderTheSway_clean a
    left join cr5222g_utsw b
    on a.inn2 = b.inn2 and a.dt = b.dt and a.crit = b.crit and a.inn1 = b.inn1
) d
where (crit not in ('5.2.2.2_g')) or ((crit in ('5.2.2.2_g')) and (d.rn <= {})) 
""".format(schema, TOP_5222g_COUNT)
sqlContext.sql(sql_query).write.format('orc').mode('overwrite').saveAsTable('{}.UnderTheSway_clean2'.format(schema))
sql_query = """
select d.*
from 
(
    select a.*, b.rn
    from {}.InfluenceOver_clean a
    left join cr5222g_inov b
    on a.inn1 = b.inn1 and a.dt = b.dt and a.crit = b.crit  and a.inn2 = b.inn2
) d
where (crit not in ('5.2.2.2_g')) or ((crit in ('5.2.2.2_g')) and (d.rn <= {})) 
""".format(schema, TOP_5222g_COUNT)
sqlContext.sql(sql_query).write.format('orc').mode('overwrite').saveAsTable('{}.InfluenceOver_clean2'.format(schema))


# ###  2.8.3. Фильтрация 5.2.2.2 <a name="p2_8_3"></a>  

# In[10]:

sql_query = """
select d.*
from
(
    select row_number() over (partition by inn2, dt order by quantity desc) as rn
           , inn2
           , dt
           , inn1
           , quantity
           , crit
    from {}.UnderTheSway_clean2
    where crit = '5.2.2.2'
) d
""".format(schema)
cr5222_utsw = sqlContext.sql(sql_query)
cr5222_utsw.createOrReplaceTempView('cr5222_utsw')  
sql_query = """
select d.*
from
(
    select row_number() over (partition by inn1, dt order by quantity desc) as rn
           , inn1
           , dt
           , inn2
           , quantity
           , crit
    from {}.InfluenceOver_clean2
    where crit = '5.2.2.2'
) d
""".format(schema)
cr5222_inov = sqlContext.sql(sql_query)
cr5222_inov.createOrReplaceTempView('cr5222_inov') 


# In[11]:

sql_query = """
select d.*
from 
(
    select a.*, b.rn as rn2
    from {}.UnderTheSway_clean2 a
    left join cr5222_utsw b
    on a.inn2 = b.inn2 and a.dt = b.dt and a.crit = b.crit and a.inn1 = b.inn1
) d
where (crit not in ('5.2.2.2')) or ((crit in ('5.2.2.2')) and (d.rn2 <= {})) 
""".format(schema, TOP_5222_COUNT)
sqlContext.sql(sql_query).write.format('orc').mode('overwrite').saveAsTable('{}.UnderTheSway_clean'.format(schema))
sql_query = """
select d.*
from 
(
    select a.*, b.rn as rn2
    from {}.InfluenceOver_clean2 a
    left join cr5222_inov b
    on a.inn1 = b.inn1 and a.dt = b.dt and a.crit = b.crit and a.inn2 = b.inn2
) d
where (crit not in ('5.2.2.2')) or ((crit in ('5.2.2.2')) and (d.rn2 <= {})) 
""".format(schema, TOP_5222_COUNT)
sqlContext.sql(sql_query).write.format('orc').mode('overwrite').saveAsTable('{}.InfluenceOver_clean'.format(schema))


# ###  2.8.4. Фильтрация sendloans и receiveloans<a name="p2_8_4"></a>  

# In[13]:

sql_query = """
select * from {}.InfluenceOver_clean
where crit not in ('sendloans') or (crit = 'sendloans' and quantity >= {}) 
""".format(schema, MIN_SENDLOANS)
sqlContext.sql(sql_query).write.format('orc').mode('overwrite').saveAsTable('{}.InfluenceOver_clean2'.format(schema))
sql_query = """
select * from {}.UnderTheSway_clean
where crit not in ('sendloans') or (crit = 'sendloans' and quantity >= {}) 
""".format(schema, MIN_SENDLOANS)
sqlContext.sql(sql_query).write.format('orc').mode('overwrite').saveAsTable('{}.UnderTheSway_clean2'.format(schema))


# In[14]:

sql_query = """
select * from {}.InfluenceOver_clean2
where crit not in ('receiveloans') or (crit = 'receiveloans' and quantity >= {}) 
""".format(schema, MIN_RECEIVE_LOANS)
sqlContext.sql(sql_query).write.format('orc').mode('overwrite').saveAsTable('{}.InfluenceOver_clean'.format(schema))
sql_query = """
select * from {}.UnderTheSway_clean2
where crit not in ('receiveloans') or (crit = 'receiveloans' and quantity >= {}) 
""".format(schema, MIN_RECEIVE_LOANS)
sqlContext.sql(sql_query).write.format('orc').mode('overwrite').saveAsTable('{}.UnderTheSway_clean'.format(schema))


# ## 2.9. Формирование выходной витрины <a name="p2_9"></a> 

# In[22]:

sql_query = """
select 
            inn1
            ,inn2
            ,dt
            ,link_prob
            ,case when ((crit = '5.1.9') and (quantity = 0)) then '5.1.9bgr'
                 when ((crit = '5.1.9') and (quantity = 1)) then '5.1.9bhld'
                 when ((crit = '5.1.9') and (quantity = 2)) then '5.1.9both'
                 else crit end as crit    
            ,case when (crit = '5.1.9') then 1 else quantity end as quantity
            ,assets1
            ,revenue1
            ,revenue1_norm
            ,assets1_norm
            ,assets2_norm
            ,revenue2_norm
            ,assets2
            ,revenue2
            ,INN1PD
            ,INN2PD
            ,'InfluenceOver' as link_direction_flag
        from {}.InfluenceOver_clean
        where  (INN1PD is not null) and  (INN2PD is not null) and revenue1 > 1
""".format(schema)
BothLinks = sqlContext.sql(sql_query)
BothLinks.write.format('orc').mode('overwrite').saveAsTable('{}.GroupInfluenceIn'.format(schema))

sql_query = """
select 
            inn2 as inn1
            ,inn1 as inn2
            ,dt
            ,link_prob
            ,case when ((crit = '5.1.9') and (quantity = 0)) then '5.1.9bgr'
                 when ((crit = '5.1.9') and (quantity = 1)) then '5.1.9bhld'
                 when ((crit = '5.1.9') and (quantity = 2)) then '5.1.9both'
                 else crit end as crit    
            ,case when (crit = '5.1.9') then 1 else quantity end as quantity
            ,assets2 as assets1
            ,revenue2 as revenue1
            ,revenue2_norm as revenue1_norm
            ,assets2_norm as assets1_norm 
            ,assets1_norm as assets2_norm
            ,revenue1_norm as revenue2_norm
            ,assets1 as assets2
            ,revenue1 as revenue2
            ,INN2PD as INN1PD        
            ,INN1PD as INN2PD
            ,'UnderTheSway' as link_direction_flag
        from {}.UnderTheSway_clean
        where (INN1PD is not null) and  (INN2PD is not null) and revenue2 > 1
""".format(schema)
BothLinks = sqlContext.sql(sql_query)
BothLinks.write.format('orc').mode('append').saveAsTable('{}.GroupInfluenceIn'.format(schema))


# In[23]:

crits4 = sqlContext.sql("select crit, count(distinct inn1, inn2, dt) as cnt from {}.GroupInfluenceIn group by crit order by cnt desc".format(schema)).toPandas()
crits4


# # 3. Подвал -- структура входных справочников <a name="p3"></a> 

# In[62]:

sbl_crits = ['founders',
             'receiveloans',
             'docapit',    
             'nploan',
             'coworking', 
             'payoffloans', 
             'guarantors', 
             'consgr',
             'crmlk', 
             'headon_tr', 
             'adregrul', 
             'consult_in', 
             'zalogi',
             'payoffloans_out',
             'dividends_out', 
             'headonloan', 
             'sendloans', 
             'adrrosstat', 
             'dividends_in'
]
with open('sbl_crits.pkl','wb') as f:
    pickle.dump(sbl_crits,f)


# In[6]:

ul_crits = ['5.1.1', 
            '5.1.4', 
            '5.1.4g', 
            '5.1.4gd', 
            '5.1.4sd', 
            '5.1.5', 
            '5.1.5g', 
            '5.1.6', 
            '5.1.6gd', 
            '5.1.6tp', 
            '5.1.7', 
            '5.1.7ow',
            '5.1.7owgd', 
            '5.1.7rel', 
            '5.1.8', 
            '5.1.9'
           ]
with open('ul_crits.pkl','wb') as f:
    pickle.dump(ul_crits,f)


# In[7]:

el_crits = ['5.2.1.1', 
            '5.2.1.1_g', 
            '5.2.1.2',
            '5.2.1.2_gul', 
            '5.2.1.3', 
            '5.2.1.3_g', 
            '5.2.1.4', 
            '5.2.1.4_gfl', 
            '5.2.1.4_gul', 
            '5.2.2.2', 
            '5.2.2.2_g']
with open('el_crits.pkl','wb') as f:
    pickle.dump(el_crits,f)


# In[29]:

limitations_up_dict = {
    'zalogi' : 30,
    'adregrul' : 8,
    'adrrosstat' : 8,
    'headon_tr' : 1
}
with open('limitations_up.pkl','wb') as f:
    pickle.dump(limitations_up_dict,f)


# In[1]:

import pickle
limitations_down_dict = {
    'dividends_out' : 0.01, # 0.005?
    'consult_in' : 0.03,
    'dividends_in' : 0.01, 
    'headon_tr' : 0.03,
    '5.1.4g' : 0.05, 
    '5.1.5g' : 0.05, 
    '5.2.1.3_g' : 0.01,
    '5.2.2.2_g' : 0.1,
    'founders' : 0.5
}
with open('limitations_down.pkl','wb') as f:
    pickle.dump(limitations_down_dict,f)


# In[3]:

deprecated_crits = []# ['payoffloans', 'payoffloans_out']
with open('deprecated_crits.pkl','wb') as f:
    pickle.dump(deprecated_crits,f)

