package ru.sberbank.sdcb.k7m.core.pack

final case class NoSuchNodeException(private val message: String = "Неверно указан узел построения, возможные варианты: stg, ods_stg, mmz_stg, rl_stg",
																		 private val cause: Throwable = None.orNull) extends Exception(message, cause)