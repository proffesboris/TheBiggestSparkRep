package ru.sberbank.sdcb.k7m.core.pack

trait DictionaryDescription {

  val LOAD_POSTFIX = "Load"

  val config: Config

  val fileName: String

  val tableName: String

  val createTableQuery: String

  lazy val dropTableQuery: String = s"drop table if exists $tableName"

  lazy val tempFileName: String = fileName + LOAD_POSTFIX
}
