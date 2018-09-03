package ru.sberbank.sdcb.k7m.core.pack

import ru.sberbank.sdcb.k7m.core.pack.CustomLogStatus.CustomLogStatus
import ru.sberbank.sdcb.k7m.core.pack.LogStatus.LogStatus
import ru.sberbank.sdcb.k7m.core.pack.RunStatus.RunStatus

trait Logging {
  def logStart(step: String, runStatus: RunStatus, logStatus: LogStatus): Unit

  def logInserted(step: String, runStatus: RunStatus, logStatus: LogStatus, count: Long): Unit

  def logEnd(step: String, runStatus: RunStatus, logStatus: LogStatus): Unit

  def log(objectId: String, runMessage: String, status: CustomLogStatus): Unit
}

trait DummyLogger extends Logging {
  def logStart(step: String, runStatus: RunStatus, logStatus: LogStatus): Unit = ()

  def logInserted(step: String, runStatus: RunStatus, logStatus: LogStatus, count: Long): Unit = ()

  def logEnd(step: String, runStatus: RunStatus, logStatus: LogStatus): Unit = ()
}

