package ru.sberbank.sdcb.k7m.core.pack

/**
  * Конфигурация окружения
  *
  * @param stg     наименование схемы stg
  * @param aux     наименование схемы aux
  * @param pa      наименование схемы pa
  * @param bkp     наименование схемы bkp
  * @param stgPath путь схемы stg с "/" в конце
  * @param auxPath путь схемы aux с "/" в конце
  * @param paPath  путь схемы pa с "/" в конце
  * @param bkpPath путь схемы bkp с "/" в конце
  */
sealed abstract class Config(val stg: String, val aux: String,
                             val pa: String, val bkp: String,
                             val stgPath: String, val auxPath: String,
                             val paPath: String, val bkpPath: String)

/**
  * ПРОМ ОД
  */
case object PROD extends Config("custom_cb_k7m_stg", "custom_cb_k7m_aux", "custom_cb_k7m", "custom_cb_k7m_bkp",
  "/data/custom/cb/k7m/stg/", "/data/custom/cb/k7m/aux/", "/data/custom/cb/k7m/pa/", "/data/custom/cb/k7m/bkp/")

/**
  * ПРОМ ЛД
  */
case object LD extends Config("t_team_k7m_stg", "t_team_k7m_aux_d", "t_team_k7m_pa_d", "t_team_k7m_bkp",
  "/user/team/team_k7m/team_k7m_stg/hive/", "/user/team/team_k7m/development/pa/", "/user/team/team_k7m/development/aux/", "/user/team/team_k7m/bkp/")

/**
  * ДЕВ ОД
  */
case object DEV_OD extends Config("team_k7m_stg", "team_k7m_aux_d", "team_k7m_pa_d", "team_k7m_bkp_d",
  "/custom/cb/k7m/development/stg/", "/custom/cb/k7m/development/stg/", "/custom/cb/k7m/development/aux/", "/custom/cb/k7m/development/bkp/"
)

/**
  * ПРОМ ЛД*
  */
case object LD_2 extends Config("custom_cb_k7m_stg", "t_team_k7m_development", "t_team_k7m_marts", "team_k7m_bkp",
  "/data/custom/cb/k7m/stg/", "/user/team/team_k7m/team_k7m_development/hive/", "/user/team/team_k7m/team_k7m_marts/hive/", "custom/cb/k7m/development/bkp/"
)
