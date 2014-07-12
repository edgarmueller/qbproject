package org.qbproject.csv.internal

import java.io.InputStreamReader

import au.com.bytecode.opencsv.CSVReader
import org.qbproject.csv.QBResource
import org.qbproject.csv.internal.CSVColumnUtil.CSVRow
import play.api.data.validation.ValidationError
import play.api.libs.json.{JsError, JsResult}

import scala.collection.JavaConversions._
import scala.util.{Failure, Try}

class CSVAdaptRowUtil[A <: Any](
  factory: CSVRow => JsResult[A], joinKeys: Set[ReverseSplitKey] = Set.empty) extends CSVColumnUtil(factory) {

  def parse(resource: QBResource, csvChars: (Char, Char)): List[JsResult[A]] = {
    val reader = new CSVReader(new InputStreamReader(resource.inputStream, "utf-8"), csvChars._1, csvChars._2)
    val (headers :: rawRows) = reader.readAll().toList.map(_.map(_.trim).toList)
    val rows = rawRows.zipWithIndex.map(row => CSVRow(row._1, headers, resource.identifier, row._2 + 2))
    val updatedRows = rows.flatMap { row =>
      row.headers.find(header =>
        isForeignSplitKey(header)
      ).fold {
        List(row)
      } { header =>
        val indexOfHeader = headers.indexOf(header)
        val ids = row.row(indexOfHeader).split(",").toList
        ids.zip(List.fill(ids.size)(row)).map {
          idWithRow =>
            CSVRow(idWithRow._2.row.updated(indexOfHeader, idWithRow._1), headers, resource.identifier, row.rowNr)
        }
      }
    }
    convertRows(updatedRows, resource)
  }

  def isForeignSplitKey(header: String): Boolean = {
    joinKeys.map(_.secondaryKey).contains(header)
  }

  def convertRows(rows: List[CSVRow], resource: QBResource): List[JsResult[A]] = {
    rows.view.zipWithIndex.map {
      row =>
        val rowResult = Try {
          factory(row._1)
        }
        val rowNr = row._2 + 2
        // CSV parser throws exceptions
        if (rowResult.isFailure) {
          val error = rowResult.asInstanceOf[Failure[_]]
          val errorMessage = "Error in row " + rowNr + " : " +
            error.exception.getMessage + "\n"
          JsError(ValidationError(errorMessage, CSVErrorInfo(resource.identifier, rowNr)))
        } else {
          useParsedResult(rowResult.get, CSVDiagnosis(rowNr, resource.identifier))
        }
    }.toList
  }

  def useParsedResult(result: JsResult[A], csvDiagnosis: CSVDiagnosis): JsResult[A] = result

  class CSVDiagnosis(csvRow: Int, resourceIdentifier: String) {
    def row = csvRow
    def resource = resourceIdentifier
  }

  object CSVDiagnosis {
    def apply(row: Int, resource: String) = new CSVDiagnosis(row, resource)
  }

}