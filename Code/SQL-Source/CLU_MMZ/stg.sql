create table custom_cb_k7m.mmz_clu as
select
        a.u7m_id,
        max(case when b.role_id = 0 then b.mmz_off_check_zone end) as borrower_zone,
        max(case when b.role_id = 1 then b.mmz_off_check_zone end) as participant_zone,
        max(case when b.role_id = 2 then b.mmz_off_check_zone end) as guarantor_zone,
        max(case when b.role_id = 3 then b.mmz_off_check_zone end) as pledger_zone
  from
        custom_cb_k7m.clu a
        left join (
            select
                    c7m_id,
                    role_id,
                    mmz_off_check_zone,
                    row_number() over (partition by c7m_id, role_id order by load_dt desc) rn
              from
                    custom_cb_k7m_stg.mmz_mmz_k7m_out
        ) b on (a.u7m_id = b.c7m_id and b.rn = 1)
 group by
        a.u7m_id

create table custom_cb_k7m.mmz_clf as
select
        a.f7m_id,
        max(case when b.role_id = 1 then b.mmz_off_check_zone end) as participant_zone,
        max(case when b.role_id = 2 then b.mmz_off_check_zone end) as guarantor_zone,
        max(case when b.role_id = 3 then b.mmz_off_check_zone end) as pledger_zone
  from
        custom_cb_k7m.clf a
        left join (
            select
                    c7m_id,
                    role_id,
                    mmz_off_check_zone,
                    row_number() over (partition by c7m_id, role_id order by load_dt desc) rn
              from
                    custom_cb_k7m_stg.mmz_mmz_k7m_out
        ) b on (a.f7m_id = b.c7m_id and b.rn = 1)
 group by
        a.f7m_id
