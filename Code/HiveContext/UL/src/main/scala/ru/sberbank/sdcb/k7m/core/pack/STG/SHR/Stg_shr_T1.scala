package ru.sberbank.sdcb.k7m.core.pack.STG.SHR

import org.apache.spark.sql.DataFrame
import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class Stg_shr_T1(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(STG_shr_T1_ShortName)
	val dashboardPath: String = genDashBoardPath(STG_shr_T1_ShortName)

	val dataframe: DataFrame = spark.sql(s"""
SELECT
            sh2.ul_org_id,
            sh2.sh_inn, -- ИНН
            sh2.sh_ul_inn,
            cast(sh2.sh_share_prcnt_cnt as decimal(22,5)) / 100 as sh_share_prcnt_cnt,
            CASE WHEN lower(sh2.sh_type)='физическое лицо' THEN 1
                    ELSE CASE WHEN lower(sh2.sh_type)='юридическое лицо' THEN 2 else 3 END
                 END as sh_type,
                sh2.sh_nm, -- ФИО
                sh2.effectivefrom

FROM
    (
        SELECT
                sh1.ul_org_id
                ,sh1.sh_inn
                ,sh1.sh_ul_inn
                ,sh1.sh_share_prcnt_cnt
                ,sh1.sh_type
                ,sh1.sh_nm
                ,sh1.effectivefrom
        FROM

            (
                SELECT
                        sh.*,
                        row_number() over (partition by
                            sh.sh_ul_inn, sh.sh_inn, sh.sh_type
                            order by cast(sh.sh_share_prcnt_cnt as decimal(22,5)), sh.effectivefrom desc)
                            as rn

                 FROM $ZeroLayerSchema.int_shareholder sh
         ) sh1
        INNER JOIN
            (
                SELECT sh_ul_inn
                , max(effectivefrom) as effectivefrom
                FROM $ZeroLayerSchema.int_shareholder
                WHERE '$DATE' >= effectivefrom
                GROUP BY sh_ul_inn
            ) max_date
        ON sh1.sh_ul_inn = max_date.sh_ul_inn AND sh1.effectivefrom = max_date.effectivefrom
    WHERE sh1.rn=1
    ) sh2
WHERE (sh2.sh_ul_inn is not null)
    AND (sh2.sh_inn is not null)
    AND (cast(sh2.sh_inn as double) is not null)
    AND (sh2.sh_inn not like '00000000%')
    AND  (
            (
                (lower(sh2.sh_type)='физическое лицо')
                AND (length(sh2.sh_inn)=12)
            )

            OR
            (
                (lower(sh2.sh_type)='юридическое лицо')
                AND (length(sh2.sh_inn)=10)
            )
        )
    AND (sh2.sh_share_prcnt_cnt is not null)
    AND (cast(sh2.sh_share_prcnt_cnt as string)!='')
    and sh2.sh_inn <> $NRD_INN
""")

}
