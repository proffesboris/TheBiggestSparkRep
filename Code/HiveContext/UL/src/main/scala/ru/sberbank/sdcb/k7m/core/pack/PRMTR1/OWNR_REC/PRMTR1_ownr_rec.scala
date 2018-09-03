package ru.sberbank.sdcb.k7m.core.pack.PRMTR1.OWNR_REC

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class PRMTR1_ownr_rec(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(PRMTR1_ownr_rec_ShortName)
	val dashboardPath: String = genDashBoardPath(PRMTR1_ownr_rec_ShortName)

	val dds_ownr_Name: String = genDashBoardName(DDS_ownr_ShortName)

	val dataframe = spark.table(dds_ownr_Name) // ПОКА ЗАГЛУШКА

	val dds_ownr =spark.table(dds_ownr_Name)

	val dds_ownr_broadcasted = spark.sparkContext.broadcast(dds_ownr)

	dds_ownr_broadcasted.value.createOrReplaceTempView("dds_ownr")

	val PRMTR1_ownr_rec_part1 = spark.sql(
		s"""
			 |    select
			 |                        t1.inn -- ИНН
			 |                        , t1.nm -- ФИО (Добавил Семен)
			 |                        , t1.inn_established
			 |                        , cast(t1.share_sum as decimal(22,5)) as share_sum
			 |                        , t1.share_type
			 |                        from dds_ownr t1
			 |                        where t1.inn <> t1.inn_established
		""".stripMargin)

	val PRMTR1_ownr_rec_part2 = spark.sql(
		"""
			|            select
			|                    t1.inn
			|                    , t1.nm -- ФИО (Добавил Семен)
			|                    , t2.inn_established
			|                    , cast(t1.share_sum * t2.share_sum as decimal(22,5)) as share_sum
			|                    , t1.share_type
			|                   from dds_ownr t1
			|                   inner join dds_ownr t2
			|                       on (t1.inn_established = t2.inn)
			|                   where (t1.inn <> t2.inn_established)
		""".stripMargin)

	val PRMTR1_ownr_rec_part3 = spark.sql(	
		"""
			| select
			|             t1.inn
			|             , t1.nm -- ФИО (Добавил Семен)
			|             , t3.inn_established
			|             , cast(t1.share_sum * t2.share_sum * t3.share_sum as decimal(22,5)) as share_sum
			|             , t1.share_type
			|        from dds_ownr t1
			|            inner join dds_ownr t2
			|                   on t1.inn = t2.inn_established
			|            inner join dds_ownr t3
			|                   on (t2.inn = t3.inn_established)
			|            where (t1.inn <> t2.inn_established) and (t1.inn <> t3.inn_established)
		""".stripMargin)

	val PRMTR1_ownr_rec_part4 = spark.sql(
		"""
			|   select             t1.inn
			|   						, t1.nm -- ФИО (Добавил Семен)
			|                , t4.inn_established
			|                , cast(t1.share_sum * t2.share_sum * t3.share_sum * t4.share_sum as decimal(22,5)) as share_sum
			|                , t1.share_type
			|                from dds_ownr t1
			|                inner join dds_ownr t2
			|                   on t1.inn_established = t2.inn
			|                inner join dds_ownr t3
			|                   on t2.inn_established = t3.inn
			|                inner join dds_ownr t4
			|                   on t3.inn_established = t4.inn
			|                where (t1.inn <> t2.inn_established)
			|                    and (t1.inn <> t3.inn_established)
			|                    and (t1.inn <> t4.inn_established)
		""".stripMargin)

	val PRMTR1_ownr_rec_part5 = spark.sql(
		"""
			|     select       t1.inn
			|     , t1.nm -- ФИО (Добавил Семен)
			|            , t5.inn_established
			|            , cast(t1.share_sum * t2.share_sum * t3.share_sum * t4.share_sum * t5.share_sum as decimal(22,5)) as share_sum
			|            , t1.share_type
			|        from dds_ownr t1
			|        inner join dds_ownr t2
			|            on t1.inn_established = t2.inn
			|        inner join dds_ownr t3
			|            on t2.inn_established = t3.inn
			|        inner join dds_ownr t4
			|            on t3.inn_established = t4.inn
			|        inner join dds_ownr t5
			|            on t4.inn_established = t5.inn
			|
			|        where (t1.inn <> t2.inn_established)
			|                   and (t1.inn <> t3.inn_established)
			|                   and (t1.inn <> t4.inn_established)
			|                   and (t1.inn <> t5.inn_established)
		""".stripMargin)

	val PRMTR1_ownr_rec_part6 = spark.sql(
		"""
			|select
			|             t1.inn
			|             , t1.nm -- ФИО (Добавил Семен)
			|             , t6.inn_established
			|             , cast(t1.share_sum * t2.share_sum * t3.share_sum * t4.share_sum * t5.share_sum * t6.share_sum as decimal(22,5)) as share_sum
			|             , t1.share_type
			|                   from dds_ownr t1
			|                   inner join dds_ownr t2
			|                   on t1.inn_established = t2.inn
			|                   inner join dds_ownr t3
			|                   on t2.inn_established = t3.inn
			|                   inner join dds_ownr t4
			|                   on t3.inn_established = t4.inn
			|                   inner join dds_ownr t5
			|                   on t4.inn_established = t5.inn
			|                   inner join dds_ownr t6
			|                   on t5.inn_established = t6.inn
			|        where (t1.inn <> t2.inn_established)
			|                   and (t1.inn <> t3.inn_established)
			|                   and (t1.inn <> t4.inn_established)
			|                   and (t1.inn <> t5.inn_established)
			|                   and (t1.inn <> t6.inn_established)
		""".stripMargin)

	val PRMTR1_ownr_rec_part7 = spark.sql(
		"""
			| select
			|             t1.inn
			|             , t1.nm -- ФИО (Добавил Семен)
			|             , t7.inn_established
			|             , cast(t1.share_sum * t2.share_sum * t3.share_sum * t4.share_sum * t5.share_sum * t6.share_sum * t7.share_sum as decimal(22,5)) as share_sum
			|             , t1.share_type
			|             from dds_ownr t1
			|             inner join dds_ownr t2         on t1.inn_established = t2.inn
			|                   inner join dds_ownr t3
			|                       on t2.inn_established = t3.inn
			|                   inner join dds_ownr t4
			|                       on t3.inn_established = t4.inn
			|                   inner join dds_ownr t5
			|                       on t4.inn_established = t5.inn
			|                   inner join dds_ownr t6
			|                       on t5.inn_established = t6.inn
			|                   inner join dds_ownr t7
			|                       on t6.inn_established = t7.inn
			|    where (t1.inn <> t2.inn_established)
			|            and (t1.inn <> t3.inn_established)
			|            and (t1.inn <> t4.inn_established)
			|            and (t1.inn <> t5.inn_established)
			|            and (t1.inn <> t6.inn_established)
			|            and (t1.inn <> t7.inn_established)
		""".stripMargin)

	val PRMTR1_ownr_rec_part8 = spark.sql(
		"""
			| select
			|             t1.inn
			|             , t1.nm -- ФИО (Добавил Семен)
			|             , t8.inn_established
			|             , cast(t1.share_sum * t2.share_sum * t3.share_sum * t4.share_sum * t5.share_sum * t6.share_sum * t7.share_sum * t8.share_sum as decimal(22,5)) as share_sum
			|             , t1.share_type
			|                   from dds_ownr t1
			|                   inner join dds_ownr t2
			|                   on t1.inn_established = t2.inn
			|                   inner join dds_ownr t3
			|                   on t2.inn_established = t3.inn
			|                   inner join dds_ownr t4
			|                   on t3.inn_established = t4.inn
			|                   inner join dds_ownr t5
			|                   on t4.inn_established = t5.inn
			|                   inner join dds_ownr t6
			|                   on t5.inn_established = t6.inn
			|                   inner join dds_ownr t7
			|                   on t6.inn_established = t7.inn
			|                   inner join dds_ownr t8
			|                   on t7.inn_established = t8.inn
			|
			|            where (t1.inn <> t2.inn_established)
			|                   and (t1.inn <> t3.inn_established)
			|                   and (t1.inn <> t4.inn_established)
			|                   and (t1.inn <> t5.inn_established)
			|                   and (t1.inn <> t6.inn_established)
			|                   and (t1.inn <> t7.inn_established)
			|                   and (t1.inn <> t8.inn_established)
		""".stripMargin)

	val PRMTR1_ownr_rec_part9 = spark.sql(
		"""
			| select
			|                 t1.inn
			|                 , t1.nm -- ФИО (Добавил Семен)
			|                   , t9.inn_established
			|                   , cast(t1.share_sum * t2.share_sum * t3.share_sum * t4.share_sum * t5.share_sum * t6.share_sum *
			|                          t7.share_sum * t8.share_sum * t9.share_sum as decimal(22,5)) as share_sum
			|                   , t1.share_type
			|                   from dds_ownr t1
			|                   inner join dds_ownr t2
			|                   on t1.inn_established = t2.inn
			|                   inner join dds_ownr t3
			|                   on t2.inn_established = t3.inn
			|                   inner join dds_ownr t4
			|                   on t3.inn_established = t4.inn
			|                   inner join dds_ownr t5
			|                   on t4.inn_established = t5.inn
			|                   inner join dds_ownr t6
			|                   on t5.inn_established = t6.inn
			|                   inner join dds_ownr t7
			|                   on t6.inn_established = t7.inn
			|                   inner join dds_ownr t8
			|                   on t7.inn_established = t8.inn
			|                   inner join dds_ownr t9
			|                   on t8.inn_established = t9.inn
			|                   where (t1.inn <> t2.inn_established)
			|                       and (t1.inn <> t3.inn_established)
			|                       and (t1.inn <> t4.inn_established)
			|                       and (t1.inn <> t5.inn_established)
			|                       and (t1.inn <> t6.inn_established)
			|                       and (t1.inn <> t7.inn_established)
			|                       and (t1.inn <> t8.inn_established)
			|                       and (t1.inn <> t9.inn_established)
		""".stripMargin)

	val PRMTR1_ownr_rec_part10 = spark.sql(
		"""
			|select
			|            t1.inn
			|            , t1.nm -- ФИО (Добавил Семен)
			|            , t10.inn_established
			|            , cast(t1.share_sum * t2.share_sum * t3.share_sum
			|                                                                * t4.share_sum * t5.share_sum * t6.share_sum
			|                                                                * t7.share_sum * t8.share_sum * t9.share_sum
			|                                                                * t10.share_sum as decimal(22,5)) as share_sum
			|                   , t1.share_type
			|                   from dds_ownr t1
			|                   inner join dds_ownr t2
			|                   on t1.inn_established = t2.inn
			|                   inner join dds_ownr t3
			|                   on t2.inn_established = t3.inn
			|                   inner join dds_ownr t4
			|                   on t3.inn_established = t4.inn
			|                   inner join dds_ownr t5
			|                   on t4.inn_established = t5.inn
			|                   inner join dds_ownr t6
			|                   on t5.inn_established = t6.inn
			|                   inner join dds_ownr t7
			|                   on t6.inn_established = t7.inn
			|                   inner join dds_ownr t8
			|                   on t7.inn_established = t8.inn
			|                   inner join dds_ownr t9
			|                   on t8.inn_established = t9.inn
			|                   inner join dds_ownr t10
			|                   on t9.inn_established = t10.inn
			|                   where (t1.inn <> t2.inn_established)
			|                           and (t1.inn <> t3.inn_established)
			|                           and (t1.inn <> t4.inn_established)
			|                           and (t1.inn <> t5.inn_established)
			|                           and (t1.inn <> t6.inn_established)
			|                           and (t1.inn <> t7.inn_established)
			|                           and (t1.inn <> t8.inn_established)
			|                           and (t1.inn <> t9.inn_established)
			|                           and (t1.inn <> t10.inn_established)
		""".stripMargin)

	val PRMTR1_ownr_rec_part11 = spark.sql(
		"""
			|select
			|         t1.inn, -- ИНН
			|         t1.nm -- ФИО (Добавил Семен)
			|         , t11.inn_established
			|         , cast(t1.share_sum * t2.share_sum
			|                                      * t3.share_sum * t4.share_sum * t5.share_sum * t6.share_sum
			|                                      * t7.share_sum * t8.share_sum * t9.share_sum * t10.share_sum
			|                                      * t11.share_sum as decimal(22,5)) as share_sum
			|        , t1.share_type
			|    from dds_ownr t1
			|                   inner join dds_ownr t2
			|                   on t1.inn_established = t2.inn
			|                   inner join dds_ownr t3
			|                   on t2.inn_established = t3.inn
			|                   inner join dds_ownr t4
			|                   on t3.inn_established = t4.inn
			|                   inner join dds_ownr t5
			|                   on t4.inn_established = t5.inn
			|                   inner join dds_ownr t6
			|                   on t5.inn_established = t6.inn
			|                   inner join dds_ownr t7
			|                   on t6.inn_established = t7.inn
			|                   inner join dds_ownr t8
			|                   on t7.inn_established = t8.inn
			|                   inner join dds_ownr t9
			|                   on t8.inn_established = t9.inn
			|                   inner join dds_ownr t10
			|                   on t9.inn_established = t10.inn
			|                   inner join dds_ownr t11
			|                   on t10.inn_established = t11.inn where t1.inn <> t2.inn_established
			|                   and t1.inn <> t3.inn_established and t1.inn <> t4.inn_established
			|                   and t1.inn <> t5.inn_established and t1.inn <> t6.inn_established
			|                   and t1.inn <> t7.inn_established and t1.inn <> t8.inn_established
			|                   and t1.inn <> t9.inn_established and t1.inn <> t10.inn_established
			|                   and t1.inn <> t11.inn_established
		""".stripMargin)

	def create(): Unit = {
		PRMTR1_ownr_rec_part1.write.mode("overwrite").format("parquet").option("path", dashboardPath).saveAsTable(dashboardName)
		PRMTR1_ownr_rec_part2.write.mode("append").format("parquet").option("path", dashboardPath).insertInto(dashboardName)
		PRMTR1_ownr_rec_part3.write.mode("append").format("parquet").option("path", dashboardPath).insertInto(dashboardName)
		PRMTR1_ownr_rec_part4.write.mode("append").format("parquet").option("path", dashboardPath).insertInto(dashboardName)
		PRMTR1_ownr_rec_part5.write.mode("append").format("parquet").option("path", dashboardPath).insertInto(dashboardName)
		PRMTR1_ownr_rec_part6.write.mode("append").format("parquet").option("path", dashboardPath).insertInto(dashboardName)
		PRMTR1_ownr_rec_part7.write.mode("append").format("parquet").option("path", dashboardPath).insertInto(dashboardName)
		PRMTR1_ownr_rec_part8.write.mode("append").format("parquet").option("path", dashboardPath).insertInto(dashboardName)
		PRMTR1_ownr_rec_part9.write.mode("append").format("parquet").option("path", dashboardPath).insertInto(dashboardName)
		PRMTR1_ownr_rec_part10.write.mode("append").format("parquet").option("path", dashboardPath).insertInto(dashboardName)
		PRMTR1_ownr_rec_part11.write.mode("append").format("parquet").option("path", dashboardPath).insertInto(dashboardName)
	}

}
