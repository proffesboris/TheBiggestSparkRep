Create database if not exists custom_cb_preapproval_mdb location '/data/custom/cb/preapproval_mdb/pa/snp';
Create database if not exists custom_cb_preapproval_mdb_hist location '/data/custom/cb/preapproval_mdb/pa/hist';
CREATE ROLE r_internal_eks_ibs_ro;
CREATE ROLE r_custom_cb_preapproval_mdb_rw;
CREATE ROLE r_custom_cb_preapproval_mdb_hist_rw;
GRANT SELECT ON DATABASE internal_eks_ibs to role r_internal_eks_ibs_ro;
GRANT ALL ON DATABASE custom_cb_preapproval_mdb to role r_custom_cb_preapproval_mdb_rw;
GRANT ALL ON DATABASE custom_cb_preapproval_mdb_hist to role r_custom_cb_preapproval_mdb_hist_rw;
GRANT ALL ON URI '/data/custom/cb/preapproval_mdb/pa/snp ' to role r_custom_cb_preapproval_mdb_rw;
GRANT ALL ON URI '/data/custom/cb/preapproval_mdb/pa/hist' to role r_custom_cb_preapproval_mdb_hist_rw;
GRANT ROLE r_internal_eks_ibs_ro TO GROUP g_dc_d_internal_eks_ibs_ro;
GRANT ROLE r_custom_corp_preap_smb_snp_rw TO GROUP g_dc_d_custom_cb_preapproval_mdb_rw;
GRANT ROLE r_custom_corp_preap_smb_hist_rw TO GROUP g_dc_d_custom_cb_preapproval_mdb_hist_rw;