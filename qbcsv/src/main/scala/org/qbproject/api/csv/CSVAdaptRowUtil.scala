package org.qbproject.api.csv

import org.qbproject.api.csv.CSVColumnUtil.CSVRow
import org.qbproject.csv.{CSVErrorInfo, SplitForeignJoinKey}
import play.api.libs.json.{JsPath, JsSuccess, JsError, JsResult}
import java.io.InputStreamReader
import au.com.bytecode.opencsv.CSVReader
import scala.util.{Try, Failure}
import play.api.data.validation.ValidationError
import scala.collection.JavaConversions._

class CSVAdaptRowUtil[A <: Any](
   factory: CSVRow => JsResult[A], joinKeys: Set[SplitForeignJoinKey] = Set.empty) extends CSVColumnUtil(factory) {

  def isSplitForeignJoinKey(header: String): Boolean = {
    joinKeys.map(_.foreignKey).contains(header)
  }

  // TODO: refactor splitting
  def parse(resource: QBResource, separator: Char, quoteChar: Char): List[JsResult[A]] = {
    val reader = new CSVReader(new InputStreamReader(resource.inputStream, "utf-8"), separator, quoteChar)
    val (headers :: rawRows) = reader.readAll().toList.map(_.map(_.trim).toList)
    val rows = rawRows.map(row => CSVRow(row, headers))
    val updatedRows = rows.flatMap { row =>
      row.headers.find(header =>
        isSplitForeignJoinKey(header)
      ).fold {
        List(row)
      } { header =>
        val indexOfHeader = headers.indexOf(header)
        val ids = row.row(indexOfHeader).split(",").toList
        ids.zip(List.fill(ids.size)(row)).map {
          idWithRow =>
            CSVRow(idWithRow._2.row.updated(indexOfHeader, idWithRow._1), headers)
        }
      }
    }

    convertRows(updatedRows, resource)
  }


  def convertRows(rows: List[CSVRow], resource: QBResource): List[JsResult[A]] = {
    rows.view.zipWithIndex.map {
      row =>
        val rowResult = Try {
          factory(row._1)
        }
        // CSV parser throws exceptions
        if (rowResult.isFailure) {
          val error = rowResult.asInstanceOf[Failure[_]]
          val errorMessage = "Error in row " + (row._2 + 2) + " : " +
            error.exception.getMessage + "\n"
          JsError(ValidationError(errorMessage, CSVErrorInfo(resource, row._2)))
        } else {
          // mix in resources in case of errors
          rowResult.get match {
            case success: JsSuccess[A] => success
            case error: JsError => error.prepend(JsPath(), ValidationError("Adaption error", CSVErrorInfo(resource, row._2)))
          }
        }
    }.toList
  }
}