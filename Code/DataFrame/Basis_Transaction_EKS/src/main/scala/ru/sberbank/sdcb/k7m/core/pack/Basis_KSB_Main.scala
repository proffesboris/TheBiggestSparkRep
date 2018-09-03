package ru.sberbank.sdcb.k7m.core.pack

import ru.sberbank.sdcb.k7m.core.pack.myutils.Log
import ru.sberbank.sdcb.k7m.core.pack.tables._

object Basis_KSB_Main extends BaseMainClass{

	override def run(params: Map[String, String], config: Config): Unit = {

		val source = params("source") // basis_client or clu

		Log.printStages()
		val table1 = new S1ClientFilterBeg(config, source); Log.printStats(table1.dashboardName); table1.saveAndLog()
		val table2 = new S2AcFinFilter(config); Log.printStats(table2.dashboardName); table2.saveAndLog()
		val table3 = new S3MainDocumFilter(config, source); Log.printStats(table3.dashboardName); table3.saveAndLog()
		val table4 = new S4MainDocumJoinDicts(config); Log.printStats(table4.dashboardName); table4.saveAndLog()
		val dt = new S5DebitSelect(config); Log.printStats(dt.dashboardName); dt.saveAndLog()
		val kt = new S6CreditSelect(config); Log.printStats(kt.dashboardName); kt.saveAndLog()
		val distclient = new S7DebitUnionCredit(config); Log.printStats(distclient.dashboardName); distclient.saveAndLog()
		val innsec = new S8ClientFilterEnd(config); Log.printStats(innsec.dashboardName); innsec.saveAndLog()
		val table5 = new S9FinalBasis(config, source); Log.printStats(table5.dashboardName); table5.saveAndLog()


	}
}
