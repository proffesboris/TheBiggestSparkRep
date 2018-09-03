package ru.sberbank.sdcb.k7m.core.pack

final class NoDataException(private val table: String,
                            private val cause: Throwable = None.orNull) extends Exception(s"No data in the table $table!", cause)