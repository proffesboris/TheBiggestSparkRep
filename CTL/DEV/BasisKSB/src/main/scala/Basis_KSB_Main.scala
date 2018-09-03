import My_Utils.{Log, Variables}
import Tables._

object Basis_KSB_Main extends  Variables{
	def main(args: Array[String]): Unit = {

		Log.printStages()
		val table1 = new ClientFilterBeg; Log.printStats(table1.name); table1.save()
		val table2 = new AcFinFilter; Log.printStats(table2.name); table2.save()
		val table3 = new MainDocumFilter; Log.printStats(table3.name); table3.save()
	  val table4 = new MainDocumJoinDicts; Log.printStats(table4.name); table4.save()
		val dt = new DebitSelect; Log.printStats(dt.name); dt.save()
		val kt = new KreditSelect; Log.printStats(kt.name); kt.save()
		val distclient = new DebitUnionKredit; Log.printStats(distclient.name); distclient.save()
		val innsec = new ClientFilterEnd; Log.printStats(innsec.name); innsec.save()
		val table5 = new FinalBasis; Log.printStats(table5.name); table5.save()

		table1.drop(); table2.drop(); table3.drop(); table4.drop(); dt.drop(); kt.drop(); distclient.drop(); innsec.drop()

	}
}
