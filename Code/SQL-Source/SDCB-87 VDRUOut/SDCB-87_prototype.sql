

--01 ���������� ����� ����������� �����. ��� ������ ����� ���� ����������� ����� ���
drop table if exists t_team_k7m_aux_p.VDRLOUT_LKC_01;
create table t_team_k7m_aux_p.VDRLOUT_LKC_01 stored as parquet
as
select 
U7M_ID_FROM,
U7M_ID as U7M_ID_TO,
CRIT_ID,
LINK_PROB
from t_team_k7m_pa_p.LKC
where T_FROM = 'CLU'
and CRIT_ID in ('5.1.1.', 
				'5.1.4.',
				'5.1.5.',
				'5.1.6.',
				'5.1.7.',
				'5.1.8.',
				'5.1.9.');
				
				
select * from  t_team_k7m_aux_p.VDRLOUT_LKC_01;				
--02 �� ������� �������� CLU ���������� ������, ��� �������
--�	���� U7M_ID �������� � ������� (1) � ���� U7M_ID_FROM
--�	������������ ���� ������ �������� FLAG_OFFLINE_MARK = 0
--� �������� ���� RU_ID
--  ���� � ����� ������������ = 1
drop table if exists t_team_k7m_aux_p.VDRLOUT_CLU_02;
create table t_team_k7m_aux_p.VDRLOUT_CLU_02 stored as parquet
as 
select   clu.CRM_ID
        ,clu.INN
        ,clu.OGRN
        ,clu.KPP
        ,clu.RU_ID
        ,1 as ROLE

from t_team_k7m_pa_p.CLU_87 as clu
    join t_team_k7m_aux_p.VDRLOUT_LKC_01 on clu.U7M_ID = VDRLOUT_LKC_01.U7M_ID_FROM
    where clu.RU_ID is not null
    and FLAG_OFFLINE_MARK = 0


--select * from t_team_k7m_aux_p.VDRLOUT_CLU_02

-- 03 �� ������� �������� CLU ���������� ������, ��� �������
--�	���� U7M_ID �������� � ������� (1) � ���� U7M_ID_FROM
--�	������������ ���� ������ �������� FLAG_OFFLINE_MARK = 0
--   ���� � ����� ������������ = 2
drop table if exists t_team_k7m_aux_p.VDRLOUT_CLU_03;
create table t_team_k7m_aux_p.VDRLOUT_CLU_03 stored as parquet
as 
select   clu.CRM_ID
        ,clu.INN
        ,clu.OGRN
        ,clu.KPP
        ,clu.RU_ID
        ,1 as ROLE

from t_team_k7m_pa_p.CLU_87 as CLU
    join t_team_k7m_aux_p.VDRLOUT_LKC_01 on clu.U7M_ID = VDRLOUT_LKC_01.U7M_ID_TO
    where clu.RU_ID is not null
     and FLAG_OFFLINE_MARK = 0


--4) �������� (2) � (3) ������������, ������ �������������� ����� �� ������ ��������� ������ �� ����� U7M_ID (������������ �������)
--���� ���������� ��� � ���� ��������� (2) � (3), �� ���� � ����� ������������ = 3
--������� (4) ����������� � ���� 0002
drop table if exists t_team_k7m_aux_p.VDRLOUT_CLU_04;
create table t_team_k7m_aux_p.VDRLOUT_CLU_04 stored as parquet
as 
select   clu.CRM_ID
        ,clu.INN
        ,clu.OGRN
        ,clu.KPP
        ,clu.RU_ID
        ,case when sum(clu.role) >= 3 then 3 else sum(clu.role) end

from (select * from t_team_k7m_aux_p.VDRLOUT_CLU_02
union all
select * from t_team_k7m_aux_p.VDRLOUT_CLU_03) CLU
group by clu.CRM_ID
        ,clu.INN
        ,clu.OGRN
        ,clu.KPP
        ,clu.RU_ID;

select * from t_team_k7m_aux_p.VDRLOUT_CLU_04

-- 5) ������ ��� ���� ������ ����� (1), � ������� �� �������� ������������, ������ ��� �� ���� FLAG_OFFLINE_MARK.
-- �� ���� ����� ��������, � ������������ ���.


-- 6) �� �������� (4) �������� ������� �� �����
-- ������� (6) ����������� � ���� 0003

