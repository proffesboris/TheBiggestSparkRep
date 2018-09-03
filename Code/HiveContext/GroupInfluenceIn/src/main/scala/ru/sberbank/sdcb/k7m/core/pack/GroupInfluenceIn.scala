package ru.sberbank.sdcb.k7m.core.pack

import java.time.temporal.TemporalAdjusters.lastDayOfYear
import java.time.temporal.TemporalAdjusters.firstDayOfYear

import org.apache.spark.sql.{SaveMode, SparkSession}

class GroupInfluenceIn(override val spark: SparkSession, val config: Config) extends EtlJob with EtlLogger {



  //---------------------------------------------------------

  case class Table(schema: String, name: String, select: String) {
    override def toString: String = s"$schema.$name"
  }

  override val processName = "GroupInfluenceIn"
  var dashboardName = s"${config.pa}.GroupInfluenceIn"

  val revenue_down_limit = 1000000
  val REVENUE_CUT_OFF_LOW = 0.1
  val REVENUE_CUT_OFF_HIGH = 3
  val CONST_REVENUE_UP_LIMIT = 1.2
  val CONST_REVENUE_DOWN_LIMIT = 0.8

  val date = LoggerUtils.obtainRunDtTimestamp(spark, config.aux).toLocalDateTime.toLocalDate
  val dateMinusYear = date.minusYears(1)

  val date_mass_cond_to =  date.toString//"2018-04-01"
  val date_mass_cond_from = dateMinusYear.toString  //"2017-04-01"
  val year_cond = dateMinusYear.getYear + " год"  //"2017 год"
  val year_cond_beflast = dateMinusYear.getYear - 1 + " год"  //"2016 год"
  val date_cond_LY_to = dateMinusYear.`with`(lastDayOfYear()).toString//"2017-12-31"
  val date_cond_LY_from = dateMinusYear.`with`(firstDayOfYear()).toString//"2017-01-01"
  val tab_pdstandalone = s"${config.pa}.pdstandalone"
  val tab_pdeksin2 = s"${config.pa}.pdeksin2"
 // val tab_thresholds = s"${config.aux}.dict_lk_thresholds"
  val owner = config.aux
  val aux_d = config.aux
  val pa_d = config.pa
  val stg = config.stg

  val limit519bgr = 200
  val TOP_5222g_COUNT = 9
  val TOP_5222_COUNT = 3

  val MIN_SENDLOANS = 0.02
  val MIN_RECEIVE_LOANS = 0.01

  def run() {

    logStart()
    
    import spark.sql

   /* dashboardName = s"$owner.k7m_links"

    sql(s"""drop table if exists $owner.k7m_links""")
    sql(s"""create table $owner.k7m_links as
                      select distinct
                       lk.u7m_id_from
                      , coalesce(clu1.inn,clf1.inn) inn1
                      , lk.u7m_id_to
                      , coalesce(clu2.inn,clf2.inn) inn2
                      , lkc.crit_id as crit
                      , lkc.link_prob
                      , lkc.quantity
                      from $pa_d.lk lk
                      join $pa_d.lkc lkc on lk.lk_id =lkc.lk_id
                      left join $pa_d.clu clu1 on clu1.u7m_id = lk.u7m_id_from and lk.t_from = 'CLU'
                      left join $pa_d.clf clf1 on clf1.f7m_id = lk.u7m_id_from and lk.t_from = 'CLF'
                      left join $pa_d.clu clu2 on clu2.u7m_id = lk.u7m_id_to  and lk.t_to = 'CLU'
                      left join $pa_d.clf clf2 on clf2.f7m_id = lk.u7m_id_from and lk.t_to = 'CLF'""")

    logInserted()

    dashboardName = s"$owner.all_crit"

    sql(s"""drop table if exists $owner.all_crit""")
    sql(s"""create table $owner.all_crit as
                      select l.* from $owner.k7m_links l
                       join $tab_thresholds d on lower(l.crit) = d.crit_code
                       where ((quantity <= d.max_val) and (quantity >= d.min_val) or quantity is null)
                                              and (length(inn1) = 10 or length(inn1) = 12)
                                              and (length(inn2) = 10 or length(inn2) = 12)
                                              and inn1 not like '000%'
                                              and inn2 not like '000%'
                                              and inn1 <> '7707083893'
                                              and inn2 <> '7707083893'""")

    logInserted()*/

    print("Срезы ЕГРИП")

    dashboardName = s"$owner.egrip"

    sql(s"""drop table if exists $owner.egrip""")
    sql(s"""create table $owner.egrip as
                      select a.ip_inn from
                                  (select
                                              row_number() over (partition by ip_org_id order by e.effectivefrom desc) as rn,
                                              ip_inn,
                                              ip_active_flg
                                          from $stg.int_ip_organization_egrip e
                                          where
                                              (e.effectivefrom  < '$date_mass_cond_to') and
                                              (e.effectiveto  > '$date_mass_cond_to')
                                      ) a
                                      where a.rn = 1 and a.ip_active_flg = 1
                              """)

    logInserted()

    print("Разделение на таблицы UnderTheSway и InfluenceOver-направление")

    sql(s"""drop table if exists $owner.all_crits""")
    sql(s"""
			create table $owner.all_crits as
			select a.inn_from as inn1, a.inn_to inn2, a.quantity, a.crit_id crit, a.link_prob
			from $aux_d.LK_LKC_RAW a
			--join $aux_d.crit_znr b on trim(lower(a.crit_id)) = trim(lower(b.crit))
			join $stg.rdm_link_criteria_mast b on trim(lower(a.crit_id)) = trim(lower(b.code)) and b.GL_flag = '1'
   """)//TODO Необходимо добавить в справочник признак вхождения критерия в список критериев для модели групового влияния

    dashboardName = s"$owner.all_crit_sc"

 sql(s"""drop table if exists $owner.all_crit_sc""")
 sql(s"""
   create table $owner.all_crit_sc as
	select distinct t1.inn1, t1.inn2, t1.quantity, t1.crit, t1.link_prob
		from
			(
			  select inn1, inn2, quantity, crit , link_prob
			  from $owner.all_crits a
			  where length(inn1) = 10 and length(inn2) = 10
			union all
			  select inn1, inn2, quantity, crit , link_prob
			  from $owner.all_crits a
			  join $owner.egrip p on p.ip_inn = a.inn2 and length(a.inn2) = 12
			  where length(a.inn1) = 10
			union all
			  select inn1, inn2, quantity, crit , link_prob
			  from $owner.all_crits a
			  join $owner.egrip p on p.ip_inn = a.inn1 and length(a.inn1) = 12
			  where length(a.inn2) = 10
			union all
			select inn1, inn2, quantity, crit , link_prob
			  from $owner.all_crits a
			  join $owner.egrip p1 on p1.ip_inn = a.inn1
			  join $owner.egrip p2 on p2.ip_inn = a.inn2
			  where length(inn1) = 12 and length(inn2) = 12
            ) t1
 """)

    logInserted()

    dashboardName = s"$owner.UnderTheSway"

    sql(s"""drop table if exists $owner.UnderTheSway""")
    sql(s"""create table $owner.UnderTheSway as
                      select a.inn1, a.inn2, a.quantity, a.crit, a.link_prob
                                      from $owner.all_crit_sc a
                                      inner join $aux_d.basis_client b
                                      on a.inn2 = b.org_inn_crm_num
                      """)

    logInserted()

    dashboardName = s"$owner.InfluenceOver"

    sql(s"""drop table if exists $owner.InfluenceOver""")
    sql(s"""create table $owner.InfluenceOver as
                      select a.inn1, a.inn2, a.quantity, a.crit, a.link_prob
                                      from $owner.all_crit_sc a
                                      inner join $aux_d.basis_client b
                                      on a.inn1 = b.org_inn_crm_num
                      """)

    logInserted()

    print(" 2.3. Расширенный базис -- BasisExtended ")

    dashboardName = s"$owner.inn"

    sql(s"""drop table if exists $owner.inn""")
    sql(s"""create table $owner.inn  as
                  select distinct inn  from
                  (
                  select  inn1 as inn from $owner.UnderTheSway
                  union all
                  select  inn2 as inn from $owner.InfluenceOver
                  union all
                  select  org_inn_crm_num as inn from $aux_d.basis_client
                  ) t """)

    logInserted()


    print(" 2.4. Расчет выручки (по витрине размеченных транзакций) ")
    print("2.4.2. Выручка Росстат за предыдущий календарный год (или предшествующий ему) ")

    dashboardName = s"$owner.revRO_df"

    sql(s"""drop table if exists $owner.revRO_df""")
    sql(s"""create table $owner.revRO_df as
                      select
                                   dd.inn
                                   ,greatest(max(dd.rev_last), max(dd.rev_beflast)) as revenue_rosstat
                          from (
                              select
                                      c.inn
                                      , case when period_nm = '$year_cond' and revenue_rosstat is null then 0
                                             when period_nm = '$year_cond_beflast' and revenue_rosstat is not null then revenue_rosstat
                                          else 0 end as rev_beflast
                                      , case when period_nm = '$year_cond_beflast' and revenue_rosstat is null then 0
                                             when period_nm = '$year_cond' and revenue_rosstat is not null then revenue_rosstat
                                          else 0 end as rev_last
                              from (
                                      select
                                                  b.ul_inn as inn
                                                  , period_nm
                                                  , max(cast(r.fs_value as decimal(22,5))) as revenue_rosstat
                                      from
                                          (
                                              select ul_org_id, fs_value, period_nm
                                              from
                                              $stg.int_financial_statement_rosstat
                                              where  (
                                                         (fs_line_num = '2110')
                                                          and (period_nm in ('$year_cond', '$year_cond_beflast'))
                                                          and (fs_line_cd = 'P21103')
                                                      )
                                          ) r

                                      left join
                                      (
                                          select a.ul_org_id, a.ul_inn
                                          from (
                                                  select
                                                      row_number() over (partition by ul_org_id order by effectivefrom desc) as rn,
                                                      t.*
                                                      from $stg.int_ul_organization_rosstat t
                                                      where effectivefrom < '$date_mass_cond_to'
                                                       and effectiveto > '$date_mass_cond_to'
                                    ) a
                                                  where a.rn = 1
                                      ) b

                                      on r.ul_org_id = b.ul_org_id
                                      inner join  ( select inn from $owner.inn ) ef  on b.ul_inn =  ef.inn
                                      group by b.ul_inn, period_nm
                              ) c
                          ) dd
                          group by dd.inn""")

    logInserted()

    dashboardName = s"$owner.revRO_test"

    sql(s"""drop table if exists $owner.revRO_test""")
    sql(s"""create table $owner.revRO_test as
                      select
                              inn

                              , case when revenue_rosstat < '$revenue_down_limit' then 0 else revenue_rosstat end as revenue_rosstat
                       from $owner.revRO_df """)

    logInserted()

    print("2.4.3. Выручка за предыдущий календарный год по транзакциям")

    dashboardName = s"$owner.revLY_DF"

    sql(s"""drop table if exists $owner.revLY_DF""")
    sql(s"""create table $owner.revLY_DF as
                      select  i.inn,
                               sum(cast(v.c_sum_nt as decimal(23,5))) as revenue_LY

                      from $tab_pdeksin2 v
                      join $owner.inn i on v.c_kl_kt_2_inn = i.inn
                      where v.PREDICTED_VALUE in (
                                                      'эквайринг',
                                                      'инкассация',
                                                      'оплата по договору',
                                                      'лизинг',
                                                      'аренда',
                                                      'дивиденды'
                                                      )
                                  and (v.c_date_prov <= '$date_cond_LY_to')
                                  and (v.c_date_prov >= '$date_cond_LY_from')
                      group by i.inn
                         """)

    logInserted()

    dashboardName = s"$owner.revLY_test"

    sql(s"""drop table if exists $owner.revLY_test""")
    sql(s"""create table $owner.revLY_test as
                    select
                            a.inn

                            , case when (b.revenue_rosstat < 1) or (b.revenue_rosstat is null) then a.revenue_LY
                                   when  a.revenue_LY/b.revenue_rosstat > $REVENUE_CUT_OFF_HIGH then 0
                                   when  a.revenue_LY/b.revenue_rosstat < $REVENUE_CUT_OFF_LOW then 0
                                   else a.revenue_LY
                                   end as revenue_LY

                         from $owner.revLY_df a
                         left join $owner.revRO_test b
                         on a.inn = b.inn 	 """)

    logInserted()

    print(" 2.4.4. Выручка LTM ")

    dashboardName = s"$owner.revLTM_test"

    sql(s"""drop table if exists $owner.revLTM_test""")
    sql(s"""create table $owner.revLTM_test as
                    select  i.inn,
                            sum(cast(v.c_sum_nt as decimal(23,5))) as revenue_L12M

                    from $tab_pdeksin2 v
                    join $owner.inn i on lower(trim(v.c_kl_kt_2_inn)) = lower(trim(i.inn))
                    where lower(trim(v.PREDICTED_VALUE)) in (
                                                        'эквайринг',
                                                        'инкассация',
                                                        'оплата по договору',
                                                        'лизинг',
                                                        'аренда',
                                                        'дивиденды'
                                                        )

                                    and ( cast(v.c_date_prov as date) >= '$date_mass_cond_from')
                                    and ( cast(v.c_date_prov as date)< '$date_mass_cond_to')
                            group by i.inn   """)

    logInserted()

    print("2.4.5. Финальная выручка")

    dashboardName = s"$owner.jnd_revLTM_RO_LY_df"

    sql(s"""drop table if exists $owner.jnd_revLTM_RO_LY_df""")
    sql(s"""create table $owner.jnd_revLTM_RO_LY_df as
                      select
                                a.inn

                              , revenue_L12M
                              , revenue_rosstat
                              , revenue_LY
                      from $owner.inn a
                      left join $owner.revLTM_test b2 on  a.inn=b2.inn
                      left join $owner.revRO_test b1 on  a.inn=b1.inn
                      left join $owner.revLY_test b on  a.inn=b.inn """)

    logInserted()

    dashboardName = s"$owner.Revenue_Extended_BASIS"

    sql(s"""drop table if exists $owner.Revenue_Extended_BASIS""")
    sql(s"""create table $owner.Revenue_Extended_BASIS as
                    select
                            inn
                            , revenue_L12M
                            , revenue_rosstat
                            , revenue_LY
                            , cast(case when revenue_rosstat < 1 and  revenue_L12M > $revenue_down_limit then revenue_L12M
                                   when revenue_rosstat < 1 and  revenue_L12M <= $revenue_down_limit then 0
                                   when revenue_rosstat > 1 and  (alpha < $CONST_REVENUE_UP_LIMIT) and (alpha > $CONST_REVENUE_DOWN_LIMIT) then alpha * revenue_rosstat -- между 0.8 и 1.2
                                   when revenue_rosstat > 1 and  (alpha >= $CONST_REVENUE_UP_LIMIT) and alpha <= $REVENUE_CUT_OFF_HIGH then $CONST_REVENUE_UP_LIMIT * revenue_rosstat
                                   when revenue_rosstat > 1 and  (alpha <= $CONST_REVENUE_DOWN_LIMIT) and alpha >= $REVENUE_CUT_OFF_LOW then $CONST_REVENUE_DOWN_LIMIT * revenue_rosstat
                                   when revenue_rosstat > 1 and   alpha < $REVENUE_CUT_OFF_LOW  then revenue_rosstat
                                   when revenue_rosstat > 1 and   alpha > $REVENUE_CUT_OFF_HIGH  then revenue_rosstat
                                   else -99999999
                                   end as decimal(33,5)) as revenue_L12M_true
                     from (
                     select
                            inn
                            , coalesce(revenue_L12M, 0) as revenue_L12M
                            , coalesce(revenue_rosstat, 0) as revenue_rosstat
                            , coalesce(revenue_LY, 0) as  revenue_LY
                            , cast(case when coalesce(revenue_LY, 0) > 1 then coalesce(coalesce(revenue_L12M, 0) / coalesce(revenue_LY, 0) , 0)
                                else -1
                                end as decimal(38,18)) as alpha

                    from $owner.jnd_revLTM_RO_LY_df
                     ) a """)

    logInserted()

    print("2.5. Получение Активов из Интегрум")

    dashboardName = s"$owner.assRO_df"

    sql(s"""drop table if exists $owner.assRO_df""")
    sql(s"""create table $owner.assRO_df as
                    select
                                dd.inn

                                 , greatest(max(dd.ass_last), max(dd.ass_beflast)) as assets_rosstat
                        from (
                            select
                                    c.inn
                                    , case when period_nm = '$year_cond' and assets_rosstat is null then 0
                                           when period_nm = '$year_cond_beflast' and assets_rosstat is not null then assets_rosstat
                                        else 0 end as ass_beflast
                                    , case when period_nm = '$year_cond_beflast' and assets_rosstat is null then 0
                                           when period_nm = '$year_cond' and assets_rosstat is not null then assets_rosstat
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
                                            $stg.int_financial_statement_rosstat
                                            where  (
                                                       (fs_line_num = '1600')
                                                        and (period_nm in ('$year_cond', '$year_cond_beflast'))
                                                        and (fs_line_cd = 'P16003')
                                                    )
                                        ) r
                                    left join
                                    (
                                        select a.ul_org_id, a.ul_inn
                                        from (
                                                select
                                                    row_number() over (partition by ul_org_id order by effectivefrom desc) as rn,
                                                    t.*
                                                    from $stg.int_ul_organization_rosstat t
                                                    where
                                                        effectivefrom < '$date_mass_cond_to'
                                                     and effectiveto > '$date_mass_cond_to'

                                                ) a
                                                where a.rn = 1
                                    ) b
                                    on r.ul_org_id = b.ul_org_id
                                    inner join
                                                (
                                                    select inn from $owner.inn
                                                ) ef
                                    on b.ul_inn =  ef.inn
                                    group by b.ul_inn, period_nm
                            ) c
                        ) dd
                    group by dd.inn""")

    logInserted()

    dashboardName = s"$owner.assetsRO_test"

    sql(s"""drop table if exists $owner.assetsRO_test""")
    sql(s"""create table $owner.assetsRO_test as
                    select
                            inn

                            , case when assets_rosstat < $revenue_down_limit then 0 else assets_rosstat end as assets_rosstat
                     from $owner.assRO_df
                     """)

    logInserted()

    dashboardName = s"$owner.assets_true"

    sql(s"""drop table if exists $owner.assets_true""")
    sql(s"""create table $owner.assets_true as 
                    select inn_df.inn, assets_rosstat from $owner.assetsRO_test assets_df
                    right join $owner.inn inn_df on inn_df.inn = assets_df.inn """)

    logInserted()

    print(" 2.6. Обогащение расширенного базиса активами и выручкой")

    dashboardName = s"$owner.rich_inn"

    sql(s"""drop table if exists $owner.rich_inn""")
    sql(s"""create table $owner.rich_inn as
                    select
                             a.inn
                            ,b.revenue_L12M_true
                            ,c.assets_rosstat

                    from $owner.inn a
                    left join $owner.Revenue_Extended_BASIS b
                    on (a.inn = b.inn)
                    left join $owner.assets_true  c
                    on (a.inn = c.inn)  """)

    logInserted()

    print("Формирование финальной таблицы UnderTheSway")

    dashboardName = s"$owner.UnderTheSway_final"

    sql(s"""drop table if exists $owner.UnderTheSway_final""")
    sql(s"""create table $owner.UnderTheSway_final as
                    select
                            a.inn1
                            ,a.inn2

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

                        from $owner.UnderTheSway a
                        left join $owner.rich_inn b
                        on (a.inn1 = b.inn)
                        left join $owner.rich_inn c
                        on (a.inn2 = c.inn) 	""")

    logInserted()

    print("Формирование финальной таблицы InfluenceOver")

    dashboardName = s"$owner.InfluenceOver_final"

    sql(s"""drop table if exists $owner.InfluenceOver_final""")
    sql(s"""create table $owner.InfluenceOver_final as
                    select
                            a.inn1
                            ,a.inn2

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

                        from $owner.InfluenceOver a
                        left join $owner.rich_inn b
                        on (a.inn1 = b.inn)
                        left join $owner.rich_inn c
                        on (a.inn2 = c.inn) 	""")

    logInserted()

    print("2.7. Получение PD из витрины PD_Standalone ")

    dashboardName = s"$owner.InfluenceOver_final_rat"

    sql(s"""drop table if exists $owner.InfluenceOver_final_rat""")
    sql(s"""create table $owner.InfluenceOver_final_rat as
                    select
                              a.*
                            , b.pd_standalone as inn1pd
                            , d.pd_standalone as inn2pd

                    from $owner.InfluenceOver_final a
                    left join $tab_pdstandalone b on a.inn1 = b.inn
                    left join $tab_pdstandalone d on a.inn2 = d.inn
                    """)

    logInserted()

    dashboardName = s"$owner.UnderTheSway_final_rat"

    sql(s"""drop table if exists $owner.UnderTheSway_final_rat""")
    sql(s"""create table $owner.UnderTheSway_final_rat as
                    select
                            a.*
                            , b.pd_standalone as inn1pd
                            , d.pd_standalone as inn2pd

                    from $owner.UnderTheSway_final a
                    left join $tab_pdstandalone b on a.inn1 = b.inn
                    left join $tab_pdstandalone d on a.inn2 = d.inn """)

    logInserted()

    print("2.8. Фильтрация критериев")

    print(" 2.8.1. Фильтрация 5.1.9bgr ")


    sql(s"""drop table if exists $owner.UnderTheSway_clean """)
    sql(s"""create table $owner.UnderTheSway_clean as
                    select d.*
                    from
                    (
                        select a.*, link_cnt_519bgr
                        from $owner.UnderTheSway_final_rat a
                        left join
                      (select inn2, crit,  count(inn1) as link_cnt_519bgr
                        from $owner.UnderTheSway_final_rat
                        where crit = '5.1.9bgr' and quantity =0
                        group by inn2, crit) b
                        on a.inn2 = b.inn2 and a.crit = b.crit
                    ) d
                    where (crit not in ('5.1.9bgr')) or ((crit in ('5.1.9bgr')) and (link_cnt_519bgr < $limit519bgr))
                    """)


    sql(s"""drop table if exists $owner.InfluenceOver_clean""")
    sql(s"""create table $owner.InfluenceOver_clean as
select  d.*    
from 
(
    select a.*, link_cnt_519bgr
    from $owner.InfluenceOver_final_rat a
    left join 
	(select inn1, crit, count(inn2) as link_cnt_519bgr
		from $owner.InfluenceOver_final_rat
		where crit = '5.1.9bgr' and quantity =0
		group by inn1, crit
		) b
    on a.inn1 = b.inn1 and a.crit = b.crit 
) d
where (crit not in ('5.1.9bgr')) or ((crit in ('5.1.9bgr')) and (link_cnt_519bgr < $limit519bgr)) """)


    print(" 2.8.2. Фильтрация 5.2.2.2_g")


    sql(s"""drop table if exists $owner.UnderTheSway_clean2 """)
    sql(s"""create table $owner.UnderTheSway_clean2 as
select d.*
from 
(
    select a.*, b.rn
    from $owner.UnderTheSway_clean a
    left join 
	(
    select row_number() over (partition by inn2 order by quantity desc) as rn
           , inn2

           , inn1
           , quantity
           , crit
    from $owner.UnderTheSway_clean
    where crit = '5.2.2.2_g' ) b
    on a.inn2 = b.inn2 and a.crit = b.crit and a.inn1 = b.inn1 
) d
where (crit not in ('5.2.2.2_g')) or ((crit in ('5.2.2.2_g')) and (d.rn <= $TOP_5222g_COUNT)) """)



    sql(s"""drop table if exists $owner.InfluenceOver_clean2 """)
    sql(s"""create table $owner.InfluenceOver_clean2 as 
select d.*
from 
(
    select a.*, b.rn
    from $owner.InfluenceOver_clean a
    left join 
	(
    select row_number() over (partition by inn1 order by quantity desc) as rn
           , inn1

           , inn2
           , quantity
           , crit
    from $owner.InfluenceOver_clean
    where crit = '5.2.2.2_g') b
    on a.inn1 = b.inn1 and a.crit = b.crit  and a.inn2 = b.inn2
) d
where (crit not in ('5.2.2.2_g')) or ((crit in ('5.2.2.2_g')) and (d.rn <= $TOP_5222g_COUNT))
""")


    print("  2.8.3. Фильтрация 5.2.2.2")



    sql(s"""drop table if exists $owner.UnderTheSway_clean3 """)
    sql(s"""create table $owner.UnderTheSway_clean3 as
select d.*
from 
(
    select a.*, b.rn as rn2
    from $owner.UnderTheSway_clean2 a
    left join (
    select row_number() over (partition by inn2 order by quantity desc) as rn
           , inn2

           , inn1
           , quantity
           , crit
    from $owner.UnderTheSway_clean2
    where crit = '5.2.2.2'
) b
    on a.inn2 = b.inn2 and a.crit = b.crit and a.inn1 = b.inn1
) d
where (crit not in ('5.2.2.2')) or ((crit in ('5.2.2.2')) and (d.rn2 <= $TOP_5222_COUNT)) """)



    sql(s"""drop table if exists $owner.InfluenceOver_clean3 """)
    sql(s"""create table $owner.InfluenceOver_clean3 as
select d.*
from 
(
    select a.*, b.rn as rn2
    from $owner.InfluenceOver_clean2 a
    left join 
(
    select row_number() over (partition by inn1 order by quantity desc) as rn
           , inn1

           , inn2
           , quantity
           , crit
    from $owner.InfluenceOver_clean2
    where crit = '5.2.2.2'
)  b
    on a.inn1 = b.inn1  and a.crit = b.crit and a.inn2 = b.inn2
) d
where (crit not in ('5.2.2.2')) or ((crit in ('5.2.2.2')) and (d.rn2 <= $TOP_5222_COUNT))""")


    print(" 2.8.4. Фильтрация sendloans и receiveloans")

    sql(s"""drop table if exists $owner.InfluenceOver_cl2 """)
    sql(s"""create table $owner.InfluenceOver_cl2 as
select * from $owner.InfluenceOver_clean3
where 1=1
and (crit not in ('sendloans') or (crit = 'sendloans' and quantity >= $MIN_SENDLOANS)) 
and (crit not in ('receiveloans') or (crit = 'receiveloans' and quantity >= $MIN_RECEIVE_LOANS)) 
""")


    sql(s"""drop table if exists $owner.UnderTheSway_cl2 """)
    sql(s"""create table $owner.UnderTheSway_cl2 as
select * from $owner.UnderTheSway_clean3
where 1=1
and (crit not in ('sendloans') or (crit = 'sendloans' and quantity >= $MIN_SENDLOANS))
and (crit not in ('receiveloans') or (crit = 'receiveloans' and quantity >= $MIN_RECEIVE_LOANS)) 
""")


    print("2.9. Формирование выходной витрины")

    dashboardName = s"$pa_d.GroupInfluenceIn"

    sql(s"""select             c1.u7m_id as u7m_id1,
                                       c2.u7m_id as u7m_id2,
                                              inn1
                                              ,inn2
                                              , $date_mass_cond_to  as dt
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
                                          from $owner.InfluenceOver_cl2 g
                                          join $owner.clu_keys c1
                                            on g.inn1 = c1.inn
                                             and c1.closed_and_merged_to_u7m_id is null
                                          join $owner.clu_keys c2
                                           on g.inn2 = c2.inn
                                           and c2.closed_and_merged_to_u7m_id is null
                                          where   revenue1 > 1 and (INN1PD is not null) and  (INN2PD is not null)

                                  union all
                                  select
                                               c2.u7m_id as u7m_id1,
                                               c1.u7m_id  as u7m_id2,
                                              inn2 as inn1
                                              ,inn1 as inn2
                                              , $date_mass_cond_to  as dt
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
                                          from $owner.UnderTheSway_cl2 g
                                          join $owner.clu_keys c1
                                            on g.inn1 = c1.inn
                                             and c1.closed_and_merged_to_u7m_id is null
                                          join $owner.clu_keys c2
                                           on g.inn2 = c2.inn
                                           and c2.closed_and_merged_to_u7m_id is null
                                          where revenue2 > 1 and (INN1PD is not null) and  (INN2PD is not null)""")

      .write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .option("path", s"${config.paPath}GroupInfluenceIn")
      .saveAsTable(s"${config.pa}.GroupInfluenceIn")

    logInserted()
    logEnd()
  }
}