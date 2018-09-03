package ru.sberbank.sdcb.k7m.core.pack.BEN_CRM.CRM_DDS

import org.apache.spark.sql.expressions.{Window, WindowSpec}
import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

class INN_ENRICH(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(CRM_BENFR_TABLE_ShortName)
	val dashboardPath: String = genDashBoardPath(CRM_BENFR_TABLE_ShortName)

	val dataframe = spark.table(genDashBoardName(DDS_BEN_SHR_CRM_T1_ShortName)) // ПОКА ЗАГЛУШКА

	override def processName: String = "BEN"

	def create(): Unit = {

		val DDS_BEN_SHR_CRM_T1_Name = genDashBoardName(DDS_BEN_SHR_CRM_T1_ShortName)

		val tmp_mdm_inn_Name = genDashBoardName(tmp_mdm_inn_ShortName)
		val tmp_mdm_inn_Path = genDashBoardPath(tmp_mdm_inn_ShortName)

		val tmp_mdm_person_Name = genDashBoardName(tmp_mdm_person_ShortName)
		val tmp_mdm_person_Path = genDashBoardPath(tmp_mdm_person_ShortName)

		val tmp_ben_keys_Name = genDashBoardName(tmp_ben_keys_ShortName)
		val tmp_ben_keys_Path = genDashBoardPath(tmp_ben_keys_ShortName)

		val tmp_ben_keys_mdm_step1_Name = genDashBoardName(tmp_ben_keys_mdm_step1_ShortName)
		val tmp_ben_keys_mdm_step1_Path = genDashBoardPath(tmp_ben_keys_mdm_step1_ShortName)

		val tmp_ben_keys_mdm_step2_Name = genDashBoardName(tmp_ben_keys_mdm_step2_ShortName)
		val tmp_ben_keys_mdm_step2_Path = genDashBoardPath(tmp_ben_keys_mdm_step2_ShortName)

		val tmp_ben_keys_inn_Name = genDashBoardName(tmp_ben_keys_inn_ShortName)
		val tmp_ben_keys_inn_Path = genDashBoardName(tmp_ben_keys_inn_ShortName)

		val tmp_mdm_inn = spark.sql(
			s"""
				 |    select
				 |        cont_id,
				 |        max(case when id_tp_cd = 1011 then ref_num else null end) inn,
				 |        max(case when id_tp_cd = 21 then ref_num else null end) doc
				 |    from
				 |        (select
				 |              t.cont_id
				 |            , t.id_tp_cd
				 |            , t.ref_num     -- Серия и номер
				 |            , t.sb_issue_dt
				 |            , t.expiry_dt
				 |            , t.identifier_desc -- Кем выдан
				 |            , t.issue_location      -- Код подразделения Кем выдан
				 |            , row_number() over(
				 |                partition by
				 |                    t.cont_id,
				 |                    t.id_tp_cd
				 |                order by
				 |                    nvl(t.end_dt,cast('9999-12-31 00:00:00' as timestamp)) desc,
				 |                    t.start_dt desc,
				 |                    nvl(t.expiry_dt,cast('9999-12-31 00:00:00' as timestamp)) desc,
				 |                    t.sb_issue_dt desc
				 |                ) rn
				 |        from
				 |            $ZeroLayerSchema.mdm_identifier t
				 |        where t.id_tp_cd in (
				 |            21  -- Паспорт РФ
				 |            ,1011   -- ИНН
				 |            )
				 |        ) t
				 |    where rn = 1
				 |    group by cont_id
		""".stripMargin)

		save(tmp_mdm_inn, tmp_mdm_inn_Name, tmp_mdm_inn_Path)

		val tmp_mdm_person = spark.sql(
			s"""
				 |  select
				 |    t.cont_id,
				 |    t.l_name,
				 |    t.f_name,
				 |    t.m_name,
				 |    p.birth_dt
				 |from
				 |
			|       (
				 |        select *
				 |        from
				 |            (select
				 |                  t.cont_id,
				 |                  upper(trim(t.last_name))      l_name,
				 |                  upper(trim(t.given_name_one)) f_name,
				 |                  upper(trim(t.given_name_two)) m_name,
				 |                  row_number() over(partition by t.cont_id order by nvl(t.end_dt,cast('9999-12-31 00:00:00' as timestamp)) desc, t.start_dt desc) rn
				 |            from
				 |                $ZeroLayerSchema.mdm_personname t
				 |            ) t
				 |        where rn=1
				 |        ) t
				 |    join  $ZeroLayerSchema.mdm_person p
				 |    on p.cont_id = t.cont_id
		""".stripMargin)

		save(tmp_mdm_person, tmp_mdm_person_Name, tmp_mdm_person_Path)

		val tmp_ben_keys = spark.sql(
			s"""
				 |    select
				 |        distinct
				 |        fst_name ,
				 |        mid_name ,
				 |        last_name,
				 |        birth_dt,
				 |        unix_timestamp(birth_dt) b_ux,
				 |        concat(trim(upper(last_name )),trim(upper(fst_name)),coalesce(trim(upper(mid_name)),'-'), cast(unix_timestamp(birth_dt) as string)  ) key1
				 |    from $DDS_BEN_SHR_CRM_T1_Name
				 |    where fst_name is not null
				 |        and birth_dt is not null
				 |        and last_name is not null
		""".stripMargin)

		save(tmp_ben_keys, tmp_ben_keys_Name, tmp_ben_keys_Path)

		val tmp_ben_keys_mdm_step1 = spark.sql(
			s"""
				 |    select
				 |            k.key1,
				 |            k.fst_name ,
				 |            k.mid_name ,
				 |            k.last_name,
				 |            k.birth_dt,
				 |            p.cont_id
				 |    		 from $tmp_ben_keys_Name k
				 |        join $tmp_mdm_person_Name p
				 |        on concat(trim(upper(p.l_name )),trim(upper(p.f_name)),trim(  coalesce(upper(p.m_name),'-') ), cast(unix_timestamp(p.birth_dt) as string)  ) = k.key1
				 | where
				 |    p.l_name  is not null
				 |    and p.f_name is not null
				 |    and p.birth_dt is not null
		""".stripMargin)

		save(tmp_ben_keys_mdm_step1, tmp_ben_keys_mdm_step1_Name, tmp_ben_keys_mdm_step1_Path)

		val tmp_ben_keys_mdm_step2 = spark.sql(
			s"""
				 |select      m.key1,
				 |            m.fst_name ,
				 |            m.mid_name ,
				 |            m.last_name,
				 |            m.birth_dt,
				 |            m.cont_id,
				 |            i.inn,
				 |            i.doc
				 |		 from $tmp_ben_keys_mdm_step1_Name m
				 |    join $tmp_mdm_inn_Name i
				 |    on i.cont_id = m.cont_id
				 |where i.inn is not null
		""".stripMargin)

		save(tmp_ben_keys_mdm_step2, tmp_ben_keys_mdm_step2_Name, tmp_ben_keys_mdm_step2_Path)

		import spark.implicits._

		val tmp_ben_keys_tbl = spark.table(tmp_ben_keys_Name)
		val tmp_ben_keys_mdm_step2_tbl = spark.table(tmp_ben_keys_mdm_step2_Name)

		val window_function: WindowSpec = Window.partitionBy($"key1")

		val pre_tmp_ben_keys_inn = tmp_ben_keys_tbl
			.join(tmp_ben_keys_mdm_step2_tbl, tmp_ben_keys_tbl("key1") === tmp_ben_keys_mdm_step2_tbl("key1"), "left")
	  	.groupBy(
				tmp_ben_keys_tbl("key1"),
				tmp_ben_keys_tbl("fst_name"),
				tmp_ben_keys_tbl("mid_name"),
				tmp_ben_keys_tbl("last_name"),
				tmp_ben_keys_tbl("birth_dt"),
				tmp_ben_keys_mdm_step2_tbl("inn").as("inn_enrich")
			).agg(
			max(tmp_ben_keys_mdm_step2_tbl("cont_id")).as("cont_id"),
			max(tmp_ben_keys_mdm_step2_tbl("doc")).as("doc_enrich")
		)

		val tmp_ben_keys_inn = pre_tmp_ben_keys_inn
			.select(
				$"key1",
				$"fst_name",
				$"mid_name",
				$"last_name",
				$"birth_dt",
				$"cont_id",
				$"inn_enrich",
				$"doc_enrich"
			).withColumn("cnt_inn_dups", count($"inn_enrich").over(window_function))

		save(tmp_ben_keys_inn, tmp_ben_keys_inn_Name, tmp_ben_keys_inn_Path)

		val keys = spark.table(tmp_ben_keys_inn_Name)
		val crm = spark.table(DDS_BEN_SHR_CRM_T1_Name)

		val pre_dataframe = crm.join(keys, keys("key1") === concat(trim(upper(crm("last_name"))),trim(upper(crm("fst_name"))), coalesce(trim(upper(crm("mid_name"))), lit("-")), unix_timestamp(crm("birth_dt"))).cast(StringType) && keys("cnt_inn_dups") === lit(1),"left")

		val dataframe = pre_dataframe.select(crm("*"), keys("inn_enrich"), keys("doc_enrich"), keys("cnt_inn_dups"))


		save(dataframe, dashboardName, dashboardPath)

	}



}
