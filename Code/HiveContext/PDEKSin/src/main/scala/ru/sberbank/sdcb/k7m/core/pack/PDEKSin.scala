package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.SparkSession

class PDEKSin(spark: SparkSession, schema: String) {

	private val saveFormat: String = "parquet"

	val PDFOKStandaloneName = s"$schema.PDFOKStandalone"
	val PDFactorsFOKStandaloneName = s"$schema.PDFactorsFOKStandalone"
	val PDFactorsFOKRSRVStandaloneName = s"$schema.PDFactorsFOKRSRVStandalone"
	val PDPrvStandaloneName = s"$schema.PDPrvStandalone"
	val PDFactorsPrvStandaloneName = s"$schema.PDFactorsPrvStandalone"
	val PDFactorsPrvRSRVStandaloneName = s"$schema.PDFactorsPrvRSRVStandalone"
	val PDIngmStandaloneName = s"$schema.PDIngmStandalone"
	val PDFactorsIngmStandaloneName = s"$schema.PDFactorsIngmStandalone"
	val PDFactorsIngmRSRVStandaloneName = s"$schema.PDFactorsIngmRSRVStandalone"
	val PDEKSStandaloneName = s"$schema.PDEKSStandalone"
	val PDFactorsEKSStandaloneName = s"$schema.PDFactorsEKSStandalone"
	val PDFactorsEKSRSRVStandaloneName = s"$schema.PDFactorsEKSRSRVStandalone"
	val PDStandaloneName = s"$schema.PDStandalone"
	val PDFactorsStandaloneName = s"$schema.PDFactorsStandalone"
	val PDFactorsRSRVStandaloneName = s"$schema.PDFactorsRSRVStandalone"
	val PDOutName = s"$schema.PDOut"
	val PDFactorsOutName = s"$schema.PDFactorsOut"
	val PDFactorsRSRVOutName = s"$schema.PDFactorsRSRVOut"
	val ProxyRiskSegmentStandaloneName = s"$schema.ProxyRiskSegmentStandalone"
	val ProxyFactorsRiskSegmentStandaloneName = s"$schema.ProxyFactorsRiskSegmentStandalone"
	val ProxyFactorsRiskSegmentRsrvStandaloneName = s"$schema.ProxyFactorsRiskSegmentRsrvStandalone"
	val LimitOutName = s"$schema.LimitOut"
	val LimitFactorsOutName = s"$schema.LimitFactorsOut"
	val LGDOutName = s"$schema.LGDOut"
	val PDEKSInName = s"$schema.PDEKSIn"
	val PDEKSIn2Name = s"$schema.PDEKSIn2"
	val GroupInfluenceOutName = s"$schema.GroupInfluenceOut"

	val PDFOKStandalone = s"""create table if not exists $schema.PDFOKStandalone(
		U7M_ID STRING,
		CRM_ID STRING,
		FLAG_RESIDENT BIGINT,
		INN STRING,
		EXEC_ID STRING,
		model_code STRING,
		request_date TIMESTAMP,
		FOK_score DOUBLE,
		FOK_ws1 DOUBLE,
		FOK_ws2 DOUBLE,
		FOK_ws3 DOUBLE,
		FOK_ws4 DOUBLE,
		FOK_ws5 DOUBLE,
		FOK_ws6 DOUBLE,
		FOK_ws7 DOUBLE,
		FOK_ws8 DOUBLE,
		FOK_ws9 DOUBLE,
		FOK_ws10 DOUBLE,
		FOK_ws11 DOUBLE,
		FOK_ws12 DOUBLE,
		FOK_ws13 DOUBLE,
		FOK_ws14 DOUBLE,
		FOK_ws15 DOUBLE,
		FOK_ws16 DOUBLE,
		FOK_ws17 DOUBLE,
		FOK_ws18 DOUBLE,
		FOK_ws19 DOUBLE,
		FOK_ws20 DOUBLE,
		FOK_ws21 DOUBLE,
		FOK_ws22 DOUBLE,
		FOK_ws23 DOUBLE,
		FOK_ws24 DOUBLE,
		FOK_ws25 DOUBLE,
		FOK_ws26 DOUBLE,
		FOK_ws27 DOUBLE,
		FOK_ws28 DOUBLE,
		FOK_ws29 DOUBLE,
		FOK_ws30 DOUBLE,
		FOK_factor1 DOUBLE,
		FOK_factor2 DOUBLE,
		FOK_factor3 DOUBLE,
		FOK_factor4 DOUBLE,
		FOK_factor5 DOUBLE,
		FOK_factor6 DOUBLE,
		FOK_factor7 DOUBLE,
		FOK_factor8 DOUBLE,
		FOK_factor9 DOUBLE,
		FOK_factor10 DOUBLE,
		FOK_factor11 DOUBLE,
		FOK_factor12 DOUBLE,
		FOK_factor13 DOUBLE,
		FOK_factor14 DOUBLE,
		FOK_factor15 DOUBLE,
		FOK_factor16 DOUBLE,
		FOK_factor17 DOUBLE,
		FOK_factor18 DOUBLE,
		FOK_factor19 DOUBLE,
		FOK_factor20 DOUBLE,
		FOK_factor21 DOUBLE,
		FOK_factor22 DOUBLE,
		FOK_factor23 DOUBLE,
		FOK_factor24 DOUBLE,
		FOK_factor25 DOUBLE,
		FOK_factor26 DOUBLE,
		FOK_factor27 DOUBLE,
		FOK_factor28 DOUBLE,
		FOK_factor29 DOUBLE,
		FOK_factor30 DOUBLE,
		FOK_CIndex DOUBLE,
		FOK_PD DOUBLE,
		FOK_PD_price DOUBLE,
		FOK_PD_rezerv DOUBLE,
		FOK_calibr_param1 DOUBLE,
		FOK_calibr_param2 DOUBLE,
		FOK_calibr_param3 DOUBLE,
		FOK_calibr_param_price1 DOUBLE,
		FOK_calibr_param_price2 DOUBLE,
		FOK_calibr_param_price3	DOUBLE
		)"""

	val PDFactorsFOKStandalone = s"""create table if not exists $schema.PDFactorsFOKStandalone(
		U7M_ID STRING,
		EXEC_ID STRING,
		model_code STRING,
		request_date TIMESTAMP,
		KEY STRING,
		value_INT DOUBLE,
		value_CHAR STRING,
		value_DATE TIMESTAMP
		)"""

	val PDFactorsFOKRSRVStandalone = s"""create table if not exists $schema.PDFactorsFOKRSRVStandalone(
		U7M_ID STRING,
		EXEC_ID STRING,
		model_code STRING,
		request_date TIMESTAMP,
		KEY STRING,
		value_INT DOUBLE,
		value_CHAR STRING,
		value_DATE TIMESTAMP,
		name_factors STRING,
		group_factors STRING
		)"""

	val PDPrvStandalone = s"""create table if not exists $schema.PDPrvStandalone(
		U7M_ID STRING,
		CRM_ID STRING,
		FLAG_RESIDENT BIGINT,
		INN STRING,
		EXEC_ID STRING,
		model_code STRING,
		request_date TIMESTAMP,
		Prv_score DOUBLE,
		Prv_ws1  DOUBLE,
		Prv_ws2 DOUBLE,
		Prv_ws3 DOUBLE,
		Prv_ws4 DOUBLE,
		Prv_ws5 DOUBLE,
		Prv_ws6 DOUBLE,
		Prv_ws7 DOUBLE,
		Prv_ws8 DOUBLE,
		Prv_ws9 DOUBLE,
		Prv_ws10 DOUBLE,
		Prv_ws11 DOUBLE,
		Prv_ws12 DOUBLE,
		Prv_ws13 DOUBLE,
		Prv_ws14 DOUBLE,
		Prv_ws15 DOUBLE,
		Prv_ws16 DOUBLE,
		Prv_ws17 DOUBLE,
		Prv_ws18 DOUBLE,
		Prv_ws19 DOUBLE,
		Prv_ws20 DOUBLE,
		Prv_ws21 DOUBLE,
		Prv_ws22 DOUBLE,
		Prv_ws23 DOUBLE,
		Prv_ws24 DOUBLE,
		Prv_ws25 DOUBLE,
		Prv_ws26 DOUBLE,
		Prv_ws27 DOUBLE,
		Prv_ws28 DOUBLE,
		Prv_ws29 DOUBLE,
		Prv_ws30 DOUBLE,
		Prv_factor1 DOUBLE,
		Prv_factor2 DOUBLE,
		Prv_factor3 DOUBLE,
		Prv_factor4 DOUBLE,
		Prv_factor5 DOUBLE,
		Prv_factor6 DOUBLE,
		Prv_factor7 DOUBLE,
		Prv_factor8 DOUBLE,
		Prv_factor9 DOUBLE,
		Prv_factor10 DOUBLE,
		Prv_factor11 DOUBLE,
		Prv_factor12 DOUBLE,
		Prv_factor13 DOUBLE,
		Prv_factor14 DOUBLE,
		Prv_factor15 DOUBLE,
		Prv_factor16 DOUBLE,
		Prv_factor17 DOUBLE,
		Prv_factor18 DOUBLE,
		Prv_factor19 DOUBLE,
		Prv_factor20 DOUBLE,
		Prv_factor21 DOUBLE,
		Prv_factor22 DOUBLE,
		Prv_factor23 DOUBLE,
		Prv_factor24 DOUBLE,
		Prv_factor25 DOUBLE,
		Prv_factor26 DOUBLE,
		Prv_factor27 DOUBLE,
		Prv_factor28 DOUBLE,
		Prv_factor29 DOUBLE,
		Prv_factor30 DOUBLE,
		Prv_CIndex DOUBLE,
		Prv_PD DOUBLE,
		Prv_PD_price DOUBLE,
		Prv_PD_rezerv DOUBLE,
		Prv_calibr_param1 DOUBLE,
		Prv_calibr_param2 DOUBLE,
		Prv_calibr_param3 DOUBLE,
		Prv_calibr_param_price1  DOUBLE,
		Prv_calibr_param_price2 DOUBLE,
		Prv_calibr_param_price3	DOUBLE
		)"""

	val PDFactorsPrvStandalone = s"""create table if not exists $schema.PDFactorsPrvStandalone(
		U7M_ID STRING,
		EXEC_ID STRING,
		model_code STRING,
		request_date TIMESTAMP,
		KEY STRING,
		value_INT DOUBLE,
		value_CHAR STRING,
		value_DATE TIMESTAMP
		)"""

	val PDFactorsPrvRSRVStandalone = s"""create table if not exists $schema.PDFactorsPrvRSRVStandalone(
		U7M_ID STRING,
		EXEC_ID STRING,
		model_code STRING,
		request_date TIMESTAMP,
		KEY STRING,
		value_INT DOUBLE,
		value_CHAR STRING,
		value_DATE TIMESTAMP,
		name_factors STRING,
		group_factors STRING
		)"""

	val PDIngmStandalone = s"""create table if not exists $schema.PDIngmStandalone(
		U7M_ID STRING,
		CRM_ID STRING,
		FLAG_RESIDENT BIGINT,
		INN STRING,
		EXEC_ID STRING,
		model_code STRING,
		request_date TIMESTAMP,
		Ingm_score DOUBLE,
		Ingm_ws1  DOUBLE,
		Ingm_ws2 DOUBLE,
		Ingm_ws3 DOUBLE,
		Ingm_ws4 DOUBLE,
		Ingm_ws5 DOUBLE,
		Ingm_ws6 DOUBLE,
		Ingm_ws7 DOUBLE,
		Ingm_ws8 DOUBLE,
		Ingm_ws9 DOUBLE,
		Ingm_ws10 DOUBLE,
		Ingm_ws11 DOUBLE,
		Ingm_ws12 DOUBLE,
		Ingm_ws13 DOUBLE,
		Ingm_ws14 DOUBLE,
		Ingm_ws15 DOUBLE,
		Ingm_ws16 DOUBLE,
		Ingm_ws17 DOUBLE,
		Ingm_ws18 DOUBLE,
		Ingm_ws19 DOUBLE,
		Ingm_ws20 DOUBLE,
		Ingm_ws21 DOUBLE,
		Ingm_ws22 DOUBLE,
		Ingm_ws23 DOUBLE,
		Ingm_ws24 DOUBLE,
		Ingm_ws25 DOUBLE,
		Ingm_ws26 DOUBLE,
		Ingm_ws27 DOUBLE,
		Ingm_ws28 DOUBLE,
		Ingm_ws29 DOUBLE,
		Ingm_ws30 DOUBLE,
		Ingm_factor1 DOUBLE,
		Ingm_factor2 DOUBLE,
		Ingm_factor3 DOUBLE,
		Ingm_factor4 DOUBLE,
		Ingm_factor5 DOUBLE,
		Ingm_factor6 DOUBLE,
		Ingm_factor7 DOUBLE,
		Ingm_factor8 DOUBLE,
		Ingm_factor9 DOUBLE,
		Ingm_factor10 DOUBLE,
		Ingm_factor11 DOUBLE,
		Ingm_factor12 DOUBLE,
		Ingm_factor13 DOUBLE,
		Ingm_factor14 DOUBLE,
		Ingm_factor15 DOUBLE,
		Ingm_factor16 DOUBLE,
		Ingm_factor17 DOUBLE,
		Ingm_factor18 DOUBLE,
		Ingm_factor19 DOUBLE,
		Ingm_factor20 DOUBLE,
		Ingm_factor21 DOUBLE,
		Ingm_factor22 DOUBLE,
		Ingm_factor23 DOUBLE,
		Ingm_factor24 DOUBLE,
		Ingm_factor25 DOUBLE,
		Ingm_factor26 DOUBLE,
		Ingm_factor27 DOUBLE,
		Ingm_factor28 DOUBLE,
		Ingm_factor29 DOUBLE,
		Ingm_factor30 DOUBLE,
		Ingm_CIndex DOUBLE,
		Ingm_PD DOUBLE,
		Ingm_PD_price DOUBLE,
		Ingm_PD_rezerv DOUBLE,
		Ingm_calibr_param1 DOUBLE,
		Ingm_calibr_param2 DOUBLE,
		Ingm_calibr_param3 DOUBLE,
		Ingm_calibr_param_price1 DOUBLE,
		Ingm_calibr_param_price2 DOUBLE,
		Ingm_calibr_param_price3 DOUBLE
		)"""

	val PDFactorsIngmStandalone = s"""create table if not exists $schema.PDFactorsIngmStandalone(
		U7M_ID STRING,
		EXEC_ID STRING,
		model_code STRING,
		request_date TIMESTAMP,
		KEY STRING,
		value_INT DOUBLE,
		value_CHAR STRING
		)"""

	val PDFactorsIngmRSRVStandalone = s"""create table if not exists $schema.PDFactorsIngmRSRVStandalone(
		U7M_ID STRING,
		EXEC_ID STRING,
		model_code STRING,
		request_date TIMESTAMP,
		KEY STRING,
		value_INT DOUBLE,
		value_CHAR STRING,
		value_DATE TIMESTAMP,
		name_factors STRING,
		group_factors STRING
		)"""

	val PDEKSStandalone = s"""create table if not exists $schema.PDEKSStandalone(
		U7M_ID STRING,
		CRM_ID STRING,
		FLAG_RESIDENT BIGINT,
		INN STRING,
		EXEC_ID STRING,
		model_code STRING,
		request_date TIMESTAMP,
		EKS_score DOUBLE,
		EKS_ws1  DOUBLE,
		EKS_ws2 DOUBLE,
		EKS_ws3 DOUBLE,
		EKS_ws4 DOUBLE,
		EKS_ws5 DOUBLE,
		EKS_ws6 DOUBLE,
		EKS_ws7 DOUBLE,
		EKS_ws8 DOUBLE,
		EKS_ws9 DOUBLE,
		EKS_ws10 DOUBLE,
		EKS_ws11 DOUBLE,
		EKS_ws12 DOUBLE,
		EKS_ws13 DOUBLE,
		EKS_ws14 DOUBLE,
		EKS_ws15 DOUBLE,
		EKS_ws16 DOUBLE,
		EKS_ws17 DOUBLE,
		EKS_ws18 DOUBLE,
		EKS_ws19 DOUBLE,
		EKS_ws20 DOUBLE,
		EKS_ws21 DOUBLE,
		EKS_ws22 DOUBLE,
		EKS_ws23 DOUBLE,
		EKS_ws24 DOUBLE,
		EKS_ws25 DOUBLE,
		EKS_ws26 DOUBLE,
		EKS_ws27 DOUBLE,
		EKS_ws28 DOUBLE,
		EKS_ws29 DOUBLE,
		EKS_ws30 DOUBLE,
		EKS_factor1 DOUBLE,
		EKS_factor2 DOUBLE,
		EKS_factor3 DOUBLE,
		EKS_factor4 DOUBLE,
		EKS_factor5 DOUBLE,
		EKS_factor6 DOUBLE,
		EKS_factor7 DOUBLE,
		EKS_factor8 DOUBLE,
		EKS_factor9 DOUBLE,
		EKS_factor10 DOUBLE,
		EKS_factor11 DOUBLE,
		EKS_factor12 DOUBLE,
		EKS_factor13 DOUBLE,
		EKS_factor14 DOUBLE,
		EKS_factor15 DOUBLE,
		EKS_factor16 DOUBLE,
		EKS_factor17 DOUBLE,
		EKS_factor18 DOUBLE,
		EKS_factor19 DOUBLE,
		EKS_factor20 DOUBLE,
		EKS_factor21 DOUBLE,
		EKS_factor22 DOUBLE,
		EKS_factor23 DOUBLE,
		EKS_factor24 DOUBLE,
		EKS_factor25 DOUBLE,
		EKS_factor26 DOUBLE,
		EKS_factor27 DOUBLE,
		EKS_factor28 DOUBLE,
		EKS_factor29 DOUBLE,
		EKS_factor30 DOUBLE,
		EKS_CIndex DOUBLE,
		EKS_PD DOUBLE,
		EKS_PD_price DOUBLE,
		EKS_PD_rezerv DOUBLE,
		EKS_calibr_param1 DOUBLE,
		EKS_calibr_param2 DOUBLE,
		EKS_calibr_param3 DOUBLE,
		EKS_calibr_param_price1 DOUBLE,
		EKS_calibr_param_price2 DOUBLE,
		EKS_calibr_param_price3 DOUBLE
		)"""

	val PDFactorsEKSStandalone = s"""create table if not exists $schema.PDFactorsEKSStandalone(
		U7M_ID STRING,
		EXEC_ID STRING,
		model_code STRING,
		request_date TIMESTAMP,
		KEY STRING,
		value_INT DOUBLE,
		value_CHAR STRING,
		value_DATE TIMESTAMP
		)"""

	val PDFactorsEKSRSRVStandalone = s"""create table if not exists $schema.PDFactorsEKSRSRVStandalone(
		U7M_ID STRING,
		EXEC_ID STRING,
		model_code STRING,
		request_date TIMESTAMP,
		KEY STRING,
		value_INT DOUBLE,
		value_CHAR STRING,
		value_DATE TIMESTAMP,
		name_factors STRING,
		group_factors STRING
		)"""

	val PDStandalone = s"""create table if not exists $schema.PDStandalone(
		U7M_ID STRING,
		CRM_ID STRING,
		FLAG_RESIDENT BIGINT,
		INN STRING,
		EXEC_ID STRING,
		model_code STRING,
		request_date TIMESTAMP,
		SCORE_ST DOUBLE,
		ws1 DOUBLE,
		ws2 DOUBLE,
		ws3 DOUBLE,
		ws4 DOUBLE,
		ws5 DOUBLE,
		ws6 DOUBLE,
		ws7 DOUBLE,
		ws8 DOUBLE,
		ws9 DOUBLE,
		ws10 DOUBLE,
		ws11 DOUBLE,
		ws12 DOUBLE,
		ws13 DOUBLE,
		ws14 DOUBLE,
		ws15 DOUBLE,
		ws16 DOUBLE,
		ws17 DOUBLE,
		ws18 DOUBLE,
		ws19 DOUBLE,
		ws20 DOUBLE,
		ws21 DOUBLE,
		ws22 DOUBLE,
		ws23 DOUBLE,
		ws24 DOUBLE,
		ws25 DOUBLE,
		ws26 DOUBLE,
		ws27 DOUBLE,
		ws28 DOUBLE,
		ws29 DOUBLE,
		ws30 DOUBLE,
		ws31 DOUBLE,
		ws32 DOUBLE,
		ws33 DOUBLE,
		ws34 DOUBLE,
		ws35 DOUBLE,
		ws36 DOUBLE,
		ws37 DOUBLE,
		ws38 DOUBLE,
		ws39 DOUBLE,
		ws40 DOUBLE,
		ws41 DOUBLE,
		ws42 DOUBLE,
		ws43 DOUBLE,
		ws44 DOUBLE,
		ws45 DOUBLE,
		ws46 DOUBLE,
		ws47 DOUBLE,
		ws48 DOUBLE,
		ws49 DOUBLE,
		ws50 DOUBLE,
		ws51 DOUBLE,
		ws52 DOUBLE,
		ws53 DOUBLE,
		ws54 DOUBLE,
		ws55 DOUBLE,
		ws56 DOUBLE,
		ws57 DOUBLE,
		ws58 DOUBLE,
		ws59 DOUBLE,
		ws60 DOUBLE,
		ws61 DOUBLE,
		ws62 DOUBLE,
		ws63 DOUBLE,
		ws64 DOUBLE,
		ws65 DOUBLE,
		ws66 DOUBLE,
		ws67 DOUBLE,
		ws68 DOUBLE,
		ws69 DOUBLE,
		ws70 DOUBLE,
		ws71 DOUBLE,
		ws72 DOUBLE,
		ws73 DOUBLE,
		ws74 DOUBLE,
		ws75 DOUBLE,
		ws76 DOUBLE,
		ws77 DOUBLE,
		ws78 DOUBLE,
		ws79 DOUBLE,
		ws80 DOUBLE,
		ws81 DOUBLE,
		ws82 DOUBLE,
		ws83 DOUBLE,
		ws84 DOUBLE,
		ws85 DOUBLE,
		ws86 DOUBLE,
		ws87 DOUBLE,
		ws88 DOUBLE,
		ws89 DOUBLE,
		ws90 DOUBLE,
		ws91 DOUBLE,
		ws92 DOUBLE,
		ws93 DOUBLE,
		ws94 DOUBLE,
		ws95 DOUBLE,
		ws96 DOUBLE,
		ws97 DOUBLE,
		ws98 DOUBLE,
		ws99 DOUBLE,
		ws100 DOUBLE,
		factor1 DOUBLE,
		factor2 DOUBLE,
		factor3 DOUBLE,
		factor4 DOUBLE,
		factor5 DOUBLE,
		factor6 DOUBLE,
		factor7 DOUBLE,
		factor8 DOUBLE,
		factor9 DOUBLE,
		factor10 DOUBLE,
		factor11 DOUBLE,
		factor12 DOUBLE,
		factor13 DOUBLE,
		factor14 DOUBLE,
		factor15 DOUBLE,
		factor16 DOUBLE,
		factor17 DOUBLE,
		factor18 DOUBLE,
		factor19 DOUBLE,
		factor20 DOUBLE,
		factor21 DOUBLE,
		factor22 DOUBLE,
		factor23 DOUBLE,
		factor24 DOUBLE,
		factor25 DOUBLE,
		factor26 DOUBLE,
		factor27 DOUBLE,
		factor28 DOUBLE,
		factor29 DOUBLE,
		factor30 DOUBLE,
		factor31 DOUBLE,
		factor32 DOUBLE,
		factor33 DOUBLE,
		factor34 DOUBLE,
		factor35 DOUBLE,
		factor36 DOUBLE,
		factor37 DOUBLE,
		factor38 DOUBLE,
		factor39 DOUBLE,
		factor40 DOUBLE,
		factor41 DOUBLE,
		factor42 DOUBLE,
		factor43 DOUBLE,
		factor44 DOUBLE,
		factor45 DOUBLE,
		factor46 DOUBLE,
		factor47 DOUBLE,
		factor48 DOUBLE,
		factor49 DOUBLE,
		factor50 DOUBLE,
		factor51 DOUBLE,
		factor52 DOUBLE,
		factor53 DOUBLE,
		factor54 DOUBLE,
		factor55 DOUBLE,
		factor56 DOUBLE,
		factor57 DOUBLE,
		factor58 DOUBLE,
		factor59 DOUBLE,
		factor60 DOUBLE,
		factor61 DOUBLE,
		factor62 DOUBLE,
		factor63 DOUBLE,
		factor64 DOUBLE,
		factor65 DOUBLE,
		factor66 DOUBLE,
		factor67 DOUBLE,
		factor68 DOUBLE,
		factor69 DOUBLE,
		factor70 DOUBLE,
		factor71 DOUBLE,
		factor72 DOUBLE,
		factor73 DOUBLE,
		factor74 DOUBLE,
		factor75 DOUBLE,
		factor76 DOUBLE,
		factor77 DOUBLE,
		factor78 DOUBLE,
		factor79 DOUBLE,
		factor80 DOUBLE,
		factor81 DOUBLE,
		factor82 DOUBLE,
		factor83 DOUBLE,
		factor84 DOUBLE,
		factor85 DOUBLE,
		factor86 DOUBLE,
		factor87 DOUBLE,
		factor88 DOUBLE,
		factor89 DOUBLE,
		factor90 DOUBLE,
		factor91 DOUBLE,
		factor92 DOUBLE,
		factor93 DOUBLE,
		factor94 DOUBLE,
		factor95 DOUBLE,
		factor96 DOUBLE,
		factor97 DOUBLE,
		factor98 DOUBLE,
		factor99 DOUBLE,
		factor100 DOUBLE,
		CIndex_st DOUBLE,
		PDStandalone DOUBLE,
		PDStandalone_Price DOUBLE,
		PDStandalone_Reserv DOUBLE,
		calibr_param1 DOUBLE,
		calibr_param2 DOUBLE,
		calibr_param3 DOUBLE,
		calibr_param_price1 DOUBLE,
		calibr_param_price2 DOUBLE,
		calibr_param_price3 DOUBLE,
		calibr_param_st1 DOUBLE,
		calibr_param_st2 DOUBLE,
		calibr_param_st3 DOUBLE
		)"""

	val PDFactorsStandalone = s"""create table if not exists $schema.PDFactorsStandalone(
		U7M_ID STRING,
		EXEC_ID STRING,
		model_code STRING,
		request_date TIMESTAMP,
		KEY STRING,
		value_INT DOUBLE,
		value_CHAR STRING,
		value_DATE TIMESTAMP
		)"""

	val PDFactorsRSRVStandalone = s"""create table if not exists $schema.PDFactorsRSRVStandalone(
		U7M_ID STRING,
		EXEC_ID STRING,
		model_code STRING,
		request_date TIMESTAMP,
		KEY STRING,
		value_INT DOUBLE,
		value_CHAR STRING,
		value_DATE TIMESTAMP,
		name_factors STRING,
		group_factors STRING
		)"""

	val PDOut = s"""create table if not exists $schema.PDOut(
		U7M_ID STRING,
		CRM_ID STRING,
		FLAG_RESIDENT BIGINT,
		INN STRING,
		EXEC_ID STRING,
		model_code STRING,
		request_date TIMESTAMP,
		SCORE_ST DOUBLE,
		SCORE DOUBLE,
		ws1 DOUBLE,
		ws2 DOUBLE,
		ws3 DOUBLE,
		ws4 DOUBLE,
		ws5 DOUBLE,
		ws6 DOUBLE,
		ws7 DOUBLE,
		ws8 DOUBLE,
		ws9 DOUBLE,
		ws10 DOUBLE,
		ws11 DOUBLE,
		ws12 DOUBLE,
		ws13 DOUBLE,
		ws14 DOUBLE,
		ws15 DOUBLE,
		ws16 DOUBLE,
		ws17 DOUBLE,
		ws18 DOUBLE,
		ws19 DOUBLE,
		ws20 DOUBLE,
		ws21 DOUBLE,
		ws22 DOUBLE,
		ws23 DOUBLE,
		ws24 DOUBLE,
		ws25 DOUBLE,
		ws26 DOUBLE,
		ws27 DOUBLE,
		ws28 DOUBLE,
		ws29 DOUBLE,
		ws30 DOUBLE,
		ws31 DOUBLE,
		ws32 DOUBLE,
		ws33 DOUBLE,
		ws34 DOUBLE,
		ws35 DOUBLE,
		ws36 DOUBLE,
		ws37 DOUBLE,
		ws38 DOUBLE,
		ws39 DOUBLE,
		ws40 DOUBLE,
		ws41 DOUBLE,
		ws42 DOUBLE,
		ws43 DOUBLE,
		ws44 DOUBLE,
		ws45 DOUBLE,
		ws46 DOUBLE,
		ws47 DOUBLE,
		ws48 DOUBLE,
		ws49 DOUBLE,
		ws50 DOUBLE,
		ws51 DOUBLE,
		ws52 DOUBLE,
		ws53 DOUBLE,
		ws54 DOUBLE,
		ws55 DOUBLE,
		ws56 DOUBLE,
		ws57 DOUBLE,
		ws58 DOUBLE,
		ws59 DOUBLE,
		ws60 DOUBLE,
		ws61 DOUBLE,
		ws62 DOUBLE,
		ws63 DOUBLE,
		ws64 DOUBLE,
		ws65 DOUBLE,
		ws66 DOUBLE,
		ws67 DOUBLE,
		ws68 DOUBLE,
		ws69 DOUBLE,
		ws70 DOUBLE,
		ws71 DOUBLE,
		ws72 DOUBLE,
		ws73 DOUBLE,
		ws74 DOUBLE,
		ws75 DOUBLE,
		ws76 DOUBLE,
		ws77 DOUBLE,
		ws78 DOUBLE,
		ws79 DOUBLE,
		ws80 DOUBLE,
		ws81 DOUBLE,
		ws82 DOUBLE,
		ws83 DOUBLE,
		ws84 DOUBLE,
		ws85 DOUBLE,
		ws86 DOUBLE,
		ws87 DOUBLE,
		ws88 DOUBLE,
		ws89 DOUBLE,
		ws90 DOUBLE,
		ws91 DOUBLE,
		ws92 DOUBLE,
		ws93 DOUBLE,
		ws94 DOUBLE,
		ws95 DOUBLE,
		ws96 DOUBLE,
		ws97 DOUBLE,
		ws98 DOUBLE,
		ws99 DOUBLE,
		ws100 DOUBLE,
		factor1 DOUBLE,
		factor2 DOUBLE,
		factor3 DOUBLE,
		factor4 DOUBLE,
		factor5 DOUBLE,
		factor6 DOUBLE,
		factor7 DOUBLE,
		factor8 DOUBLE,
		factor9 DOUBLE,
		factor10 DOUBLE,
		factor11 DOUBLE,
		factor12 DOUBLE,
		factor13 DOUBLE,
		factor14 DOUBLE,
		factor15 DOUBLE,
		factor16 DOUBLE,
		factor17 DOUBLE,
		factor18 DOUBLE,
		factor19 DOUBLE,
		factor20 DOUBLE,
		factor21 DOUBLE,
		factor22 DOUBLE,
		factor23 DOUBLE,
		factor24 DOUBLE,
		factor25 DOUBLE,
		factor26 DOUBLE,
		factor27 DOUBLE,
		factor28 DOUBLE,
		factor29 DOUBLE,
		factor30 DOUBLE,
		factor31 DOUBLE,
		factor32 DOUBLE,
		factor33 DOUBLE,
		factor34 DOUBLE,
		factor35 DOUBLE,
		factor36 DOUBLE,
		factor37 DOUBLE,
		factor38 DOUBLE,
		factor39 DOUBLE,
		factor40 DOUBLE,
		factor41 DOUBLE,
		factor42 DOUBLE,
		factor43 DOUBLE,
		factor44 DOUBLE,
		factor45 DOUBLE,
		factor46 DOUBLE,
		factor47 DOUBLE,
		factor48 DOUBLE,
		factor49 DOUBLE,
		factor50 DOUBLE,
		factor51 DOUBLE,
		factor52 DOUBLE,
		factor53 DOUBLE,
		factor54 DOUBLE,
		factor55 DOUBLE,
		factor56 DOUBLE,
		factor57 DOUBLE,
		factor58 DOUBLE,
		factor59 DOUBLE,
		factor60 DOUBLE,
		factor61 DOUBLE,
		factor62 DOUBLE,
		factor63 DOUBLE,
		factor64 DOUBLE,
		factor65 DOUBLE,
		factor66 DOUBLE,
		factor67 DOUBLE,
		factor68 DOUBLE,
		factor69 DOUBLE,
		factor70 DOUBLE,
		factor71 DOUBLE,
		factor72 DOUBLE,
		factor73 DOUBLE,
		factor74 DOUBLE,
		factor75 DOUBLE,
		factor76 DOUBLE,
		factor77 DOUBLE,
		factor78 DOUBLE,
		factor79 DOUBLE,
		factor80 DOUBLE,
		factor81 DOUBLE,
		factor82 DOUBLE,
		factor83 DOUBLE,
		factor84 DOUBLE,
		factor85 DOUBLE,
		factor86 DOUBLE,
		factor87 DOUBLE,
		factor88 DOUBLE,
		factor89 DOUBLE,
		factor90 DOUBLE,
		factor91 DOUBLE,
		factor92 DOUBLE,
		factor93 DOUBLE,
		factor94 DOUBLE,
		factor95 DOUBLE,
		factor96 DOUBLE,
		factor97 DOUBLE,
		factor98 DOUBLE,
		factor99 DOUBLE,
		factor100 DOUBLE,
		CIndex_st DOUBLE,
		CIndex DOUBLE,
		PD_standalone DOUBLE,
		PD_OFFLINE DOUBLE,
		PD_OFFLINE_PRICE DOUBLE,
		PD_OFFLINE_REZERV DOUBLE,
		calibr_param1 DOUBLE,
		calibr_param2 DOUBLE,
		calibr_param3 DOUBLE,
		calibr_param_price1 DOUBLE,
		calibr_param_price2 DOUBLE,
		calibr_param_price3 DOUBLE,
		calibr_param_st1 DOUBLE,
		calibr_param_st2 DOUBLE,
		calibr_param_st3 DOUBLE,
		RATING_OFFLINE DOUBLE,
		RATING_OFFLINE_PRICE DOUBLE,
		RATING_OFFLINE_REZERV DOUBLE
		)"""

	val PDFactorsOut = s"""create table if not exists $schema.PDFactorsOut(
		U7M_ID STRING,
		EXEC_ID STRING,
		model_code STRING,
		request_date TIMESTAMP,
		KEY STRING,
		value_INT DOUBLE,
		value_CHAR STRING,
		value_DATE TIMESTAMP
		)"""

	val PDFactorsRSRVOut = s"""create table if not exists $schema.PDFactorsRSRVOut(
		U7M_ID STRING,
		EXEC_ID STRING,
		model_code STRING,
		request_date TIMESTAMP,
		KEY STRING,
		value_INT DOUBLE,
		value_CHAR STRING,
		value_DATE TIMESTAMP,
		name_factors STRING,
		group_factors STRING
		)"""

	val ProxyRiskSegmentStandalone = s"""create table if not exists $schema.ProxyRiskSegmentStandalone(
		U7M_ID STRING,
		CRM_ID STRING,
		FLAG_RESIDENT BIGINT,
		INN STRING,
		EXEC_ID STRING,
		model_code STRING,
		request_date TIMESTAMP,
		RISK_SEGMENT_OFFLINE STRING,
		RS_Index DOUBLE
		)"""

	val ProxyFactorsRiskSegmentStandalone = s"""create table if not exists $schema.ProxyFactorsRiskSegmentStandalone(
		U7M_ID STRING,
		EXEC_ID STRING,
		model_code STRING,
		request_date TIMESTAMP,
		KEY STRING,
		value_INT DOUBLE,
		value_CHAR STRING,
		value_DATE TIMESTAMP
		)"""

	val ProxyFactorsRiskSegmentRsrvStandalone = s"""create table if not exists $schema.ProxyFactorsRiskSegmentRsrvStandalone(
		U7M_ID STRING,
		EXEC_ID STRING,
		model_code STRING,
		request_date TIMESTAMP,
		KEY STRING,
		value_INT DOUBLE,
		value_CHAR STRING,
		value_DATE TIMESTAMP,
		name_factors STRING,
		group_factors STRING
		)"""

	val LimitOut = s"""create table if not exists $schema.LimitOut(
		U7M_ID STRING,
		CRM_ID STRING,
		FLAG_RESIDENT BIGINT,
		INN STRING,
		EXEC_ID STRING,
		model_code STRING,
		request_date TIMESTAMP,
		SKE_BASE DOUBLE,
		SKE_D_0 DOUBLE,
		SKE_D_T_OFFLINE DOUBLE,
		SKE_OFFLINE DOUBLE,
		SKE_DebtCredit0 DOUBLE,
		SKE_DebtLoan0 DOUBLE,
		SKE_DebtLeasing0 DOUBLE,
		SKE_dLoans DOUBLE,
		SKE_dLising DOUBLE,
		Debt2Close1m DOUBLE,
		Debt2Close2m DOUBLE,
		Debt2Close3m DOUBLE,
		ACTREPORT_FLAG BIGINT
		)"""

	val LimitFactorsOut = s"""create table if not exists $schema.LimitFactorsOut(
		U7M_ID STRING,
		EXEC_ID STRING,
		model_code STRING,
		request_date TIMESTAMP,
		KEY STRING,
		value_INT DOUBLE,
		value_CHAR STRING,
		value_DATE TIMESTAMP
		)"""

	val LGDOut = s"""create table if not exists $schema.LGDOut(
		BO_ID STRING,
		CLIENT_ID STRING,
		probCure DOUBLE,
		probCess DOUBLE,
		probLiq DOUBLE,
		LGDcure DOUBLE,
		LGDcess DOUBLE,
		LGDliq DOUBLE,
		LGDscore DOUBLE,
		LGDcalibr DOUBLE,
		LGDfincol DOUBLE,
		LGD DOUBLE,
		LGDdt DOUBLE,
		LGDpricing DOUBLE
		)"""

	val PDEKSIn = s"""create table if not exists $schema.PDEKSIn(
		id STRING,
		ktdt STRING,
		class_id STRING,
		state_id STRING,
		c_acc_dt STRING,
		c_acc_kt STRING,
		c_date_prov STRING,
		c_kl_dt_1_1 STRING,
		c_kl_dt_1_2 STRING,
		c_kl_dt_2_1 STRING,
		c_kl_dt_2_inn STRING,
		c_kl_dt_2_2 STRING,
		c_kl_dt_2_3 STRING,
		c_kl_dt_2_kpp STRING,
		c_kl_kt_1_1 STRING,
		c_kl_kt_1_2 STRING,
		c_kl_kt_2_1 STRING,
		c_kl_kt_2_inn STRING,
		c_kl_kt_2_2 STRING,
		c_kl_kt_2_3 STRING,
		c_kl_kt_2_kpp STRING,
		c_nazn STRING,
		c_sum DOUBLE,
		c_valuta STRING,
		c_vid_doc STRING,
		c_kod_nazn_pay STRING,
		c_valuta_po STRING,
		c_sum_po DOUBLE,
		c_multicurr STRING,
		c_sum_nt DOUBLE,
		c_type_mess STRING,
		c_code_doc STRING,
		c_num_dt STRING,
		c_num_kt STRING,
		c_filial STRING,
		c_depart STRING,
		c_client_v STRING,
		C_VALUTA_LABEL STRING,
		c_valuta_po_LABEL STRING,
		c_vid_doc_LABEL STRING,
		c_kod_nazn_pay_LABEL STRING,
		inn_st STRING,
		c_filial_label STRING,
		c_multicurr_label STRING,
		c_type_mess_label STRING,
		inn_sec STRING,
		kl_2_org STRING,
		predicted_value STRING
		)"""

	val GroupInfluenceOut = s"""create table if not exists $schema.GroupInfluenceOut(
		inn1 STRING,
		inn2 STRING,
		dt TIMESTAMP,
		quantity DOUBLE,
		crit STRING,
		link_prob DOUBLE,
		assets1 DOUBLE,
		revenue1 DOUBLE,
		assets1_norm DOUBLE,
		revenue1_norm DOUBLE,
		assets2 DOUBLE,
		revenue2 DOUBLE,
		revenue2_norm DOUBLE,
		assets2_norm DOUBLE,
		inn1_CIOFF_FLG_REVENUE_LTM_IS_NULL BIGINT,
		inn1_CIOFF_FLG_ASSETS_ARE_NULL BIGINT,
		inn1_CIOFF_FLG_ROSSTAT_REV_IS_NULL BIGINT,
		inn1_CIOFF_FLG_LTM_REV_IS_NULL BIGINT,
		inn1_CIOFF_FLG_LY_REV_IS_NULL BIGINT,
		inn1_CIOFF_FLG_NORM_REV_LESS_K_ROSSTAT_REVENUE BIGINT,
		inn1_CIOFF_FLG_NORM_REV_MORE_K_ROSSTAT_REVENUE BIGINT,
		inn2_CIOFF_FLG_REVENUE_LTM_IS_NULL BIGINT,
		inn2_CIOFF_FLG_ASSETS_ARE_NULL BIGINT,
		inn2_CIOFF_FLG_ROSSTAT_REV_IS_NULL BIGINT,
		inn2_CIOFF_FLG_LTM_REV_IS_NULL BIGINT,
		inn2_CIOFF_FLG_LY_REV_IS_NULL BIGINT,
		inn2_CIOFF_FLG_NORM_REV_LESS_K_ROSSTAT_REVENUE BIGINT,
		inn2_CIOFF_FLG_NORM_REV_MORE_K_ROSSTAT_REVENUE BIGINT,
		INN1_PRV_INT_SCORE DOUBLE,
		inn1_CIOFF_INT_NODATA DOUBLE,
		inn1_CIOFF_FOK_NODATA DOUBLE,
		INN2_PRV_INT_SCORE DOUBLE,
		inn2_CIOFF_INT_NODATA DOUBLE,
		inn2_CIOFF_FOK_NODATA DOUBLE,
		INN1_EKS_PD DOUBLE,
		INN1_EKS_SCORE DOUBLE,
		INN1_CIOFF_GR DOUBLE,
		INN2_EKS_PD DOUBLE,
		INN2_EKS_SCORE DOUBLE,
		INN2_CIOFF_GR DOUBLE
		)"""

	val PDEKSIn2 = s"""create table if not exists $schema.PDEKSIn2(
		id STRING,
		ktdt STRING,
		class_id STRING,
		state_id STRING,
		c_acc_dt STRING,
		c_acc_kt STRING,
		c_date_prov STRING,
		c_kl_dt_1_1 STRING,
		c_kl_dt_1_2 STRING,
		c_kl_dt_2_1 STRING,
		c_kl_dt_2_inn STRING,
		c_kl_dt_2_2 STRING,
		c_kl_dt_2_3 STRING,
		c_kl_dt_2_kpp STRING,
		c_kl_kt_1_1 STRING,
		c_kl_kt_1_2 STRING,
		c_kl_kt_2_1 STRING,
		c_kl_kt_2_inn STRING,
		c_kl_kt_2_2 STRING,
		c_kl_kt_2_3 STRING,
		c_kl_kt_2_kpp STRING,
		c_nazn STRING,
		c_sum DOUBLE,
		c_valuta STRING,
		c_vid_doc STRING,
		c_kod_nazn_pay STRING,
		c_valuta_po STRING,
		c_sum_po DOUBLE,
		c_multicurr STRING,
		c_sum_nt DOUBLE,
		c_type_mess STRING,
		c_code_doc STRING,
		c_num_dt STRING,
		c_num_kt STRING,
		c_filial STRING,
		c_depart STRING,
		c_client_v STRING,
		C_VALUTA_LABEL STRING,
		c_valuta_po_LABEL STRING,
		c_vid_doc_LABEL STRING,
		c_kod_nazn_pay_LABEL STRING,
		inn_st STRING,
		c_filial_label STRING,
		c_multicurr_label STRING,
		c_type_mess_label STRING,
		inn_sec STRING,
		kl_2_org STRING,
		predicted_value STRING
		)"""

	private def save(schemaAndTable: String, tableCreation: String): Unit = {
		// spark.sql(s"drop table if exists $schemaAndTable")
		spark.sql(s"$tableCreation stored as $saveFormat")
	}

	def create_tables(): Unit = {
		save(PDFOKStandaloneName, PDFOKStandalone)
		save(PDFactorsFOKStandaloneName, PDFactorsFOKStandalone)
		save(PDFactorsFOKRSRVStandaloneName, PDFactorsFOKRSRVStandalone)
		save(PDPrvStandaloneName, PDPrvStandalone)
		save(PDFactorsPrvStandaloneName, PDFactorsPrvStandalone)
		save(PDFactorsPrvRSRVStandaloneName, PDFactorsPrvRSRVStandalone)
		save(PDIngmStandaloneName, PDIngmStandalone)
		save(PDFactorsIngmStandaloneName, PDFactorsIngmStandalone)
		save(PDFactorsIngmRSRVStandaloneName, PDFactorsIngmRSRVStandalone)
		save(PDEKSStandaloneName, PDEKSStandalone)
		save(PDFactorsEKSStandaloneName, PDFactorsEKSStandalone)
		save(PDFactorsEKSRSRVStandaloneName, PDFactorsEKSRSRVStandalone)
		save(PDStandaloneName, PDStandalone)
		save(PDFactorsStandaloneName, PDFactorsStandalone)
		save(PDFactorsRSRVStandaloneName, PDFactorsRSRVStandalone)
		save(PDOutName, PDOut)
		save(PDFactorsOutName, PDFactorsOut)
		save(PDFactorsRSRVOutName, PDFactorsRSRVOut)
		save(ProxyRiskSegmentStandaloneName, ProxyRiskSegmentStandalone)
		save(ProxyFactorsRiskSegmentStandaloneName, ProxyFactorsRiskSegmentStandalone)
		save(ProxyFactorsRiskSegmentRsrvStandaloneName, ProxyFactorsRiskSegmentRsrvStandalone)
		save(LimitOutName, LimitOut)
		save(LimitFactorsOutName, LimitFactorsOut)
		save(LGDOutName, LGDOut)
		save(PDEKSInName, PDEKSIn)
		save(GroupInfluenceOutName, GroupInfluenceOut)
	}


}
