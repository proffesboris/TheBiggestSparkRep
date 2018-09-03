TECH_USER=u_dc_s_k7m
TECH_USER_RO=u_dc_s_k7m_ro
BEELINE="jdbc:hive2://hw2288-02.cloud.dev.df.sbrf.ru:10000/default;principal=hive/_HOST@DEV.DF.SBRF.RU"
echo "--------------------------"
echo "Create IPA user"
echo "--------------------------"
if ipa user-find --login $TECH_USER
then
	echo "Учётная запись $TECH_USER уже существует.";
else
ipa user-add $TECH_USER --first "Kredit" --last "7Minut" --email "$TECH_USER@cm.dev.df.sbrf.ru" --title "ТУЗ проекта кредит за 7 минутЖ создани витрин и доступ к источникам" --password
fi
if ipa user-find --login TECH_USER_RO
then
	echo "Учётная запись TECH_USER_RO уже существует.";
else
ipa user-add TECH_USER_RO --first "Kredit" --last "7Minut" --email "$TECH_USER_RO@cm.dev.df.sbrf.ru" --title "ТУЗ проекта кредит за 7 минут для чтения" --password
fi
echo "--------------------------"
echo "Create IPA groups"
echo "--------------------------"
if ipa group-find --name g_dc_d_internal_mdm_mdm_ro
then
	echo "Группа g_dc_d_internal_mdm_mdm_ro уже существует.";
else
ipa group-add g_dc_d_internal_mdm_mdm_ro --desc "Чтение internal_eks_ibs"
fi
if ipa group-find --name g_dc_d_cb_akm_integrum_ro
then
	echo "Группа g_dc_d_cb_akm_integrum_ro уже существует.";
else
ipa group-add g_dc_d_cb_akm_integrum_ro --desc "Чтение internal_eks_ibs"
fi
if ipa group-find --name g_dc_d_internal_eks_ibs_ro
then
	echo "Группа g_dc_d_internal_eks_ibs_ro уже существует.";
else
ipa group-add g_dc_d_internal_eks_ibs_ro --desc "Чтение internal_eks_ibs"
fi
if ipa group-find --name g_dc_d_cb_akm_pravo_ro
then
	echo "Группа g_dc_d_cb_akm_pravo_ro уже существует.";
else
ipa group-add g_dc_d_cb_akm_pravo_ro --desc "Чтение external_pravo"
fi
if ipa group-find --name g_dc_d_external_integrum_ro
then
	echo "Группа g_dc_d_external_integrum_ro уже существует.";
else
ipa group-add g_dc_d_external_integrum_ro --desc "Чтение external_integrum"
fi
if ipa group-find --name g_dc_d_internal_amrlirt_anamod_sbrflgd_b0_ro
then
	echo "Группа g_dc_d_internal_amrlirt_anamod_sbrflgd_b0_ro уже существует.";
else
ipa group-add g_dc_d_internal_amrlirt_anamod_sbrflgd_b0_ro --desc "Чтение internal_amrlirt_anamod_sbrflgd_b0"
fi
if ipa group-find --name g_dc_d_internal_amrlirt_anamod_sbrfsc_b0_ro
then
	echo "Группа g_dc_d_internal_amrlirt_anamod_sbrfsc_b0_ro уже существует.";
else
ipa group-add g_dc_d_internal_amrlirt_anamod_sbrfsc_b0_ro --desc "Чтение amrlirt_anamod_sbrfsc_b0"
fi
if ipa group-find --name g_dc_d_internal_crm_cb_siebel_ro
then
	echo "Группа g_dc_d_internal_crm_cb_siebel_ro уже существует.";
else
ipa group-add g_dc_d_internal_crm_cb_siebel_ro --desc "Чтение crm_cb_siebel"
fi
if ipa group-find --name g_dc_d_internal_crm_fo_sber1_ro
then
	echo "Группа g_dc_d_internal_crm_fo_sber1_ro уже существует.";
else
ipa group-add g_dc_d_internal_crm_fo_sber1_ro --desc "Чтение crm_fo_sber1"
fi
if ipa group-find --name g_dc_d_internal_tsm_retail_transact_ro
then
	echo "Группа g_dc_d_internal_tsm_retail_transact_ro уже существует.";
else
ipa group-add g_dc_d_internal_tsm_retail_transact_ro --desc "Чтение tsm_retail_transact"
fi
if ipa group-find --name g_dc_d_external_fns_ro
then
	echo "Группа g_dc_d_external_fns_ro уже существует.";
else
ipa group-add g_dc_d_external_fns_ro --desc "Чтение external_fns"
fi
if ipa group-find --name g_dc_d_internal_rdm_ro
then
	echo "Группа g_dc_d_internal_rdm_ro уже существует.";
else
ipa group-add g_dc_d_internal_rdm_ro --desc "Чтение internal_rdm"
fi
if ipa group-find --name g_dc_d_ods_dm_c7m
then
	echo "Группа g_dc_d_ods_dm_c7m уже существует.";
else
ipa group-add g_dc_d_ods_dm_c7m --desc "Чтение ods_dm_c7m"
fi
if ipa group-find --name g_dc_d_custom_cb_rw
then
	echo "Группа g_dc_d_custom_cb_rw уже существует.";
else
ipa group-add g_dc_d_custom_cb_rw --desc "Чтение и запись custom_cb_k7m_stg, custom_cb_k7m_aux, custom_cb_k7m_pa, custom_cb_k7m_bkp"
fi
if ipa group-find --name g_dc_d_custom_cb_ro
then
	echo "Группа g_dc_d_custom_cb_ro уже существует.";
else
ipa group-add g_dc_d_custom_cb_ro --desc "Чтение custom_cb_k7m_pa"
fi
echo "------------------"
echo "Add user to Groups"
echo "------------------"
if ipa group-show g_dc_d_internal_eks_ibs_ro | grep $TECH_USER
then
		echo "Пользователь $TECH_USER уже состоит в группе g_dc_d_internal_eks_ibs_ro.";
else
ipa group-add-member g_dc_d_internal_eks_ibs_ro --users $TECH_USER
fi
if ipa group-show g_dc_d_cb_akm_pravo_ro | grep $TECH_USER
then
		echo "Пользователь $TECH_USER уже состоит в группе g_dc_d_cb_akm_pravo_ro.";
else
ipa group-add-member g_dc_d_cb_akm_pravo_ro --users $TECH_USER
fi
if ipa group-show g_dc_d_external_integrum_ro | grep $TECH_USER
then
		echo "Пользователь $TECH_USER уже состоит в группе g_dc_d_external_integrum_ro.";
else
ipa group-add-member g_dc_d_external_integrum_ro --users $TECH_USER
fi
if ipa group-show g_dc_d_internal_amrlirt_anamod_sbrflgd_b0_ro | grep $TECH_USER
then
		echo "Пользователь $TECH_USER уже состоит в группе g_dc_d_internal_amrlirt_anamod_sbrflgd_b0_ro.";
else
ipa group-add-member g_dc_d_internal_amrlirt_anamod_sbrflgd_b0_ro --users $TECH_USER
fi
if ipa group-show g_dc_d_internal_amrlirt_anamod_sbrfsc_b0_ro | grep $TECH_USER
then
		echo "Пользователь $TECH_USER уже состоит в группе g_dc_d_internal_amrlirt_anamod_sbrfsc_b0_ro.";
else
ipa group-add-member g_dc_d_internal_amrlirt_anamod_sbrfsc_b0_ro --users $TECH_USER
fi
if ipa group-show g_dc_d_internal_crm_cb_siebel_ro | grep $TECH_USER
then
		echo "Пользователь $TECH_USER уже состоит в группе g_dc_d_internal_crm_cb_siebel_ro.";
else
ipa group-add-member g_dc_d_internal_crm_cb_siebel_ro --users $TECH_USER
fi
if ipa group-show g_dc_d_internal_crm_fo_sber1_ro | grep $TECH_USER
then
		echo "Пользователь $TECH_USER уже состоит в группе g_dc_d_internal_crm_fo_sber1_ro.";
else
ipa group-add-member g_dc_d_internal_crm_fo_sber1_ro --users $TECH_USER
fi
if ipa group-show g_dc_d_internal_tsm_retail_transact_ro | grep $TECH_USER
then
		echo "Пользователь $TECH_USER уже состоит в группе g_dc_d_internal_tsm_retail_transact_ro.";
else
ipa group-add-member g_dc_d_internal_tsm_retail_transact_ro --users $TECH_USER
fi
if ipa group-show g_dc_d_custom_cb_rw | grep $TECH_USER
then
		echo "Пользователь $TECH_USER уже состоит в группе g_dc_d_custom_cb_rw.";
else
ipa group-add-member g_dc_d_custom_cb_rw --users $TECH_USER
fi
if ipa group-show g_dc_d_internal_mdm_mdm_ro | grep $TECH_USER
then
		echo "Пользователь $TECH_USER уже состоит в группе g_dc_d_internal_mdm_mdm_ro.";
else
ipa group-add-member g_dc_d_internal_mdm_mdm_ro  --users $TECH_USER
fi
if ipa group-show g_dc_d_external_fns_ro | grep $TECH_USER
then
		echo "Пользователь $TECH_USER уже состоит в группе g_dc_d_external_fns_ro.";
else
ipa group-add-member g_dc_d_external_fns_ro  --users $TECH_USER
fi
if ipa group-show g_dc_d_cb_akm_integrum_ro | grep $TECH_USER
then
		echo "Пользователь $TECH_USER уже состоит в группе g_dc_d_cb_akm_integrum_ro.";
else
ipa group-add-member g_dc_d_cb_akm_integrum_ro  --users $TECH_USER
fi

if ipa group-show g_dc_d_internal_rdm_ro | grep $TECH_USER
then
		echo "Пользователь $TECH_USER уже состоит в группе g_dc_d_internal_rdm_ro.";
else
ipa group-add-member g_dc_d_internal_rdm_ro  --users $TECH_USER
fi

if ipa group-show g_dc_d_ods_dm_c7m | grep $TECH_USER
then
		echo "Пользователь $TECH_USER уже состоит в группе g_dc_d_ods_dm_c7m.";
else
ipa group-add-member g_dc_d_ods_dm_c7m  --users $TECH_USER
fi

if ipa group-show g_dc_d_custom_cb_ro | grep $TECH_USER_RO
then
		echo "Пользователь $TECH_USER_RO уже состоит в группе g_dc_d_custom_cb_ro.";
else
ipa group-add-member g_dc_d_custom_cb_ro  --users $TECH_USER_RO
fi

echo "-------------------------------"
echo "Create directories in HDFS"
echo "-------------------------------"

hdfs dfs -mkdir -p /data/custom/cb/k7m/pa/export/vdcrmout
hdfs dfs -mkdir -p /data/custom/cb/k7m/pa/export/vdmmzout
hdfs dfs -mkdir -p /data/custom/cb/k7m/pa/export/vdrlout
hdfs dfs -mkdir -p /data/custom/cb/k7m/pa/export/vdokrout

hdfs dfs -mkdir -p /data/custom/cb/k7m/pa/export/vdcrmout/archive
hdfs dfs -mkdir -p /data/custom/cb/k7m/pa/export/vdmmzout/archive
hdfs dfs -mkdir -p /data/custom/cb/k7m/pa/export/vdrlout/archive
hdfs dfs -mkdir -p /data/custom/cb/k7m/pa/export/vdokrout/archive

hdfs dfs -chown -R u_dc_s_k7m /data/custom/cb/k7m/pa/export/vdcrmout
hdfs dfs -chown -R u_dc_s_k7m /data/custom/cb/k7m/pa/export/vdmmzout
hdfs dfs -chown -R u_dc_s_k7m /data/custom/cb/k7m/pa/export/vdrlout
hdfs dfs -chown -R u_dc_s_k7m /data/custom/cb/k7m/pa/export/vdokrout

hdfs dfs -chown -R u_dc_s_k7m /data/custom/cb/k7m/pa/export/vdcrmout/archive
hdfs dfs -chown -R u_dc_s_k7m /data/custom/cb/k7m/pa/export/vdmmzout/archive
hdfs dfs -chown -R u_dc_s_k7m /data/custom/cb/k7m/pa/export/vdrlout/archive
hdfs dfs -chown -R u_dc_s_k7m /data/custom/cb/k7m/pa/export/vdokrout/archive

echo "-------------------------------"
echo "Create Century roles and grants"
echo "Create Schemas used in Project "
echo "-------------------------------"
beeline -u $BEELINE -f K7M_role_model.hql