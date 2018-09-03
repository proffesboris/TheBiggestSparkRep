package ru.sberbank.sdcb.k7m.core.pack

import ru.sberbank.sdcb.k7m.core.pack.dashboards._

object AmrlirtMain extends BaseMainClass {

	override def run(params: Map[String, String], config: Config): Unit = {

	val TimeDiscount = new AmrTimeDiscount(config); TimeDiscount.saveAndLog()
	val LrParam = new AmrLrParam(config); LrParam.saveAndLog()
	val CountryCkp = new AmrCountryCkp(config); CountryCkp.saveAndLog()
	val EadPrd = new AmrEadPrd(config); EadPrd.saveAndLog()
	val CrdMode = new AmrCrdMode(config); CrdMode.saveAndLog()

	}
}
