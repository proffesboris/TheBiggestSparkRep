create view t_team_k7m_pa_p.proxy_risk_segment_in as
select
		U7M_ID,
		CRM_ID,
		EKS_ID,
		INN,
		INDUSTRY,
		OKK,
		OKVED_CRM,
		OKVED_INTEGRUM,
		REG_DATE,
		RSEGMENT
  from
		t_team_k7m_pa_p.CLU
 where
		coalesce(CRM_ID, '') <> '' or
		coalesce(INN, '') <> ''