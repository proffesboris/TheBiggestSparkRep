package ru.sberbank.sdcb.k7m.core.pack

import java.util.concurrent.Executors

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.concurrent.blocking
import scala.concurrent.{Future, Promise}
import scala.util.control.NonFatal



object Utils {

  def quoteCol = udf { str: String =>
    if (str == null) """""""" else s""""$str""""
  }

  def unescape = udf { str: String =>
    str
      .replace("""\"{""", "{")
      .replace("""}\"""", "}")
      .replace("""\"""", "\"")
      .replace("""\\""", """\""")
      .replace(""""{""", "{")
      .replace("""}"""", "}")
      .replace("""""[""", "[")
      .replace("""]""""", "]")
  }

  implicit class DataFrameFunctions(df: DataFrame) {
    def getHeader(sep: String = ";"): String = {
      df.columns.map(_.toUpperCase).mkString(sep)
    }

    def quoteStrings(): DataFrame = {
      df.schema.fields.foldLeft(df)((resDf, field) =>
        field.dataType match {
          case StringType => resDf.withColumn(field.name, quoteCol(df(field.name)))
          case _ => resDf
        })
    }

    def replace(pattern: String, replacement: String): DataFrame = {
      df.schema.fields.foldLeft(df)((resDf, field) =>
        field.dataType match {
          case StringType => resDf.withColumn(field.name, regexp_replace(df(field.name), pattern, replacement))
          case _ => resDf
        })
    }

    def formatDates(format: String): DataFrame = {
      df.schema.fields.foldLeft(df)((resDf, field) =>
        field.dataType match {
          case TimestampType | DateType => resDf.withColumn(field.name, date_format(df(field.name), format))
          case _ => resDf
        })
    }

    def formatNumbers(): DataFrame = {
      df.schema.fields.foldLeft(df)((resDf, field) =>
        field.dataType match {
          case DecimalType() | DoubleType => resDf.withColumn(field.name, regexp_replace(df(field.name), "\\.", ","))
          case _ => resDf
        })
    }

    def concat(sep: String): DataFrame = {
      df.schema.fields.foldLeft(df)((resDf, field) =>
        field.dataType match {
          case StringType => resDf
          case _ => resDf.withColumn(field.name, df(field.name).cast("string"))
        }).na.fill("").withColumn("concat", concat_ws(sep, df.columns.map(col): _*)).select("concat")
    }
  }


  val numOfIOThreads = 100

  private val ioThreadPool = Executors.newFixedThreadPool(numOfIOThreads)

  def executeBlockingIO[T](cb: => T): Future[T] = {
    val p = Promise[T]()

    ioThreadPool.execute(new Runnable {
      def run() = try {
        p.success(blocking(cb))
      }
      catch {
        case NonFatal(ex) =>
          p.failure(ex)
      }
    })

    p.future
  }


}

object ZipUtils {
  import java.io.BufferedInputStream
  import java.util.zip.{ZipEntry, ZipOutputStream, Deflater}

  import org.apache.hadoop.fs.{FileSystem, Path}

  val Buffer = 2 * 1024

  def zipFiles(out: String, files: Iterable[String], compress: Boolean = true, removeSources: Boolean = true)(implicit fs: FileSystem): Unit = {
    var data = new Array[Byte](Buffer)
    val zip = new ZipOutputStream(fs.create(new Path(out)))
    if (!compress) zip.setLevel(Deflater.NO_COMPRESSION)
    files.foreach { file =>
      zip.putNextEntry(new ZipEntry(new Path(file).getName))
      val in = new BufferedInputStream(fs.open(new Path(file)), Buffer)
      var b = in.read(data, 0, Buffer)
      while (b != -1) {
        zip.write(data, 0, b)
        b = in.read(data, 0, Buffer)
      }
      in.close()
      zip.closeEntry()
    }
    zip.close()

    if (removeSources) files.map(new Path(_)).foreach(fs.delete(_, false))
  }

  def zipDir(dir: Path, compress: Boolean = true, removeSource: Boolean = true)(implicit fs: FileSystem): Unit = {
    val files = fs.listStatus(dir).map(_.getPath.toString)
    zipFiles(s"$dir.zip", files, compress, false)
    if (removeSource) fs.delete(dir, true)
  }

}
