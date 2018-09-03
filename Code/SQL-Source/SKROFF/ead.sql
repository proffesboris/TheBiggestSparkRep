INSERT INTO custom_cb_k7m.bo 
SELECT 
        T1.EXEC_ID
        ,T1.OKR_ID
		,T1.BO_ID
		,T1.U7M_ID
		,T1.CLUST_ID
		,'CCF_C_OFFLINE' as KEY
		,CAST(CASE 
			WHEN T2.TYPE_CD = 'D' then T2.CCFp_MAX 
			ELSE 1
		END as decimal(16,4)) as VALUE_N
		,NULL as VALUE_C
		,NULL as VALUE_D
  FROM 
		custom_cb_k7m.bo_keys T1 
		INNER JOIN (
		    SELECT 
            		T_BO.BO_ID as BO_ID
            		,T_CRDMODE.TYPE_CD as TYPE_CD
            		,MAX( 
            			CAST( 
            				CASE  
            					WHEN T2.ccfp_val like '%byDuration' then CASE 
            													WHEN T_BO.DUR_U <= 12 then substr(T2.ccfp_val, 1, 3) 
            													WHEN T_BO.DUR_U > 12 then substr(T2.ccfp_val, 5, 3) 
            													END
            					ELSE ccfp_val END
            				as DECIMAL(16, 4)
            			) 
            		) as CCFp_MAX
              FROM 
                    custom_cb_k7m.bo_keys T1
            		INNER JOIN ( 
            			SELECT 
            					BO_ID
            					,max(case WHEN key='CREDIT_MODE' then value_c else null end) as CREDIT_MODE
            					,max(case WHEN key='PRODUCT' then value_c else null end) as PRODUCT
            					,max(case WHEN key='CREDIT_INSTRUMENT' then value_c else null end) as CREDIT_INSTRUMENT
            					,max(case WHEN key='DUR_U' then value_n else null end) as DUR_U
            			  FROM 
            					custom_cb_k7m.bo
            			 GROUP BY 
            					BO_ID
            		) T_BO ON (T1.BO_ID = T_BO.BO_ID)
            		INNER JOIN 
            		( 
            			SELECT 
            					CASE crd_mode_cd 
            						WHEN 'Кредитный договор' then 'Credit Contract'
            						WHEN 'НКЛ' then 'NKL'
            						WHEN 'ВКЛ' then 'VKL'
            						WHEN 'Договор об овердрафт. кредите' then 'Contract Credit Overdraft'
            					END as CREDIT_MODE
            					,CASE loan_sort_num 
            						WHEN 'Кредит' then 'Credit'
            					END as CREDIT_INSTRUMENT
            					,type_cd
            			  FROM 
            					custom_cb_k7m_aux.amr_crd_mode
            		) T_CRDMODE ON T_BO.CREDIT_MODE = T_CRDMODE.CREDIT_MODE and T_BO.CREDIT_INSTRUMENT = T_CRDMODE.CREDIT_INSTRUMENT
            		INNER JOIN (
            			SELECT 
            					case prd_name
            						when 'Корпоративное кредитование' then '1-M813DK'
            						when 'Индивидуальный овердрафт' then '1-M813EC'
            					END AS PRODUCT
            					,ccfp_val 
            			  FROM 
            					custom_cb_k7m_aux.amr_ead_prd 
            			 WHERE 
            					prd_stts = 'active'
            		) T2 ON T_BO.PRODUCT = T2.PRODUCT
             group BY   
                    T_BO.BO_ID
            		,T_CRDMODE.TYPE_CD 
		) T2 ON T1.BO_ID=T2.BO_ID
 

