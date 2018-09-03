package ru.sberbank.sdcb.k7m.core.pack

/**
  * Базовый класс для запуска расчета витрин
  */
abstract class BaseMainClass {

  val TYPE_KEY = "type"

  def main(args: Array[String]) = {
    val params = args
      .map(_.split("="))
      .filter(_.length > 0)
      .map(x => if (x.length > 1) (x(0), x(1)) else (x(0), ""))
      .toMap[String, String]
    val config = obtainConfig(params)
    run(params, config)
  }

  /**
    * Получение конфигурации окружения из параметра type
    *
    * @param params параметры
    * @return текущая конфигурация
    */
  def obtainConfig(params: Map[String, String]): Config = {
    val str = if (params.contains(TYPE_KEY)) params(TYPE_KEY) else ""
    str match {
      case null => PROD
      case "prod" => PROD
      case "ld" => LD
      case "ld2" => LD_2
      case "od" => DEV_OD
      case _ => PROD
    }
  }

  /**
    * Метод исполнения построения витрин
    *
    * @param params параметр
    * @param config конфигурация окружения
    */
  def run(params: Map[String, String], config: Config): Unit


}
