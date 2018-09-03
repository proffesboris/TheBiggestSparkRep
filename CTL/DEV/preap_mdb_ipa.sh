#!/bin/bash
echo "--------------------------"
echo "Create IPA user and groups"
echo "--------------------------"
if ipa user-find --login=$TECH_USER
then
        echo "Учётная запись $TECH_USER уже существует.";
else
		ipa user-add $TECH_USER --first "preapproval" --last "mdb" --email "$TECH_USER@cm.dev.df.sbrf.ru" --title "ТУЗ проекта предодобренные кредитные предложения КСБ для создания витрин" --password
fi
if ipa group-find g_dc_d_internal_eks_ibs_ro
then
        echo "Группа g_dc_d_internal_eks_ibs_ro уже существует.";
else
		ipa group-add g_dc_d_internal_eks_ibs_ro --desc "Чтение internal_eks_ibs"
fi
if ipa group-find g_dc_d_custom_cb_preapproval_mdb_rw
then
        echo "Группа g_dc_d_custom_cb_preapproval_mdb_rw уже существует.";
else
		ipa group-add g_dc_d_custom_cb_preapproval_mdb_rw --desc "Чтение и запись custom_cb_preapproval_mdb"
fi
if ipa group-find g_dc_d_custom_cb_preapproval_mdb_hist_rw
then
        echo "Группа g_dc_d_custom_cb_preapproval_mdb_hist_rw уже существует.";
else
		ipa group-add g_dc_d_custom_cb_preapproval_mdb_hist_rw --desc "Чтение и запись custom_cb_preapproval_mdb_hist"
fi
if ipa group-show g_dc_d_internal_eks_ibs_ro | grep $TECH_USER
then
		echo "Пользователь $TECH_USER уже состоит в группе g_dc_d_internal_eks_ibs_ro.";
else
		ipa group-add-member g_dc_d_internal_eks_ibs_ro --users $TECH_USER
fi
if ipa group-show g_dc_d_custom_cb_preapproval_mdb_rw | grep $TECH_USER
then
		echo "Пользователь $TECH_USER уже состоит в группе g_dc_d_custom_cb_preapproval_mdb_rw.";
else
		ipa group-add-member g_dc_d_custom_cb_preapproval_mdb_rw --users $TECH_USER
fi
if ipa group-show g_dc_d_custom_cb_preapproval_mdb_hist_rw | grep $TECH_USER
then
		echo "Пользователь $TECH_USER уже состоит в группе g_dc_d_custom_cb_preapproval_mdb_hist_rw.";
else
		ipa group-add-member g_dc_d_custom_cb_preapproval_mdb_hist_rw --users $TECH_USER
fi