package ru.sberbank.sdcb.k7m.core.pack

final case class FatalSourcesException(private val message: String = "Одна или несколько таблиц источников недоступны для переноса в нулевой слой обратитесь к таблице с логами",
																			 private val cause: Throwable = None.orNull) extends Exception(message, cause)