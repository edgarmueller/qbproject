package org.qbproject.csv

import play.api.libs.json._
import scalaz.Validation.fromTryCatch
import scala.util.Try
import org.qbproject.schema._
import org.qbproject.api.schema._
import org.qbproject.api.csv.CSVColumnUtil
import CSVColumnUtil._
import play.api.data.validation.ValidationError

trait CSVSchemaAdapter extends QBAdapter[CSVRow] {

  override def atPrimitive[A <: QBPrimitiveType[_]](schema: A, path: JsPath, annotations: Seq[QBAnnotation])(implicit row: CSVRow): JsResult[JsValue] = {
    fromTryCatch({
      schema match {
        case str: QBString =>
          val str = asString(path)
          if (str.length == 0 && schema.rules.exists(_.isInstanceOf[EnumRule])) {
            // do not accept empty string if schema is an enum
            JsUndefined("")
          } else {
            JsString(str)
          }
        case bool: QBBoolean => JsString(asString(path))
        case int: QBInteger => JsString(asString(path))
        case num: QBNumber => JsString(asString(path))
      }
    }).fold(throwable => {
      handleAnnotations(schema, path, annotations)
    }, JsSuccess(_))
  }

  // check optionals and defaults
  def handleAnnotations[A <: QBPrimitiveType[_]](schema: A, path: JsPath, annotations: Seq[QBAnnotation])(implicit row: CSVRow): JsResult[JsValue] = {
    def handleDefault: JsResult[JsValue] = {
      annotations.collectFirst {
        case default: QBDefaultAnnotation => default
      }.fold[JsResult[JsValue]] {
        JsError(path -> ValidationError("Expected: " + schema + " at " + path, CSVErrorInfo(row.resourceIdentifier, row.rowNr)))
      } {
        default => JsSuccess(default.value)
      }
    }
    annotations.collectFirst { case optional: QBOptionalAnnotation =>
      optional.fallBack
    }.fold[JsResult[JsValue]] {
      handleDefault
    } { possibleFallBack =>
      JsSuccess(possibleFallBack.fold[JsValue] {
        JsUndefined("")
      } {
        identity
      })
    }
  }

  override def atArray(schema: QBArray, path: JsPath, annotations: Seq[QBAnnotation])(implicit row: CSVRow): JsResult[JsValue] = {
    val csvHeader = path.toString().substring(1).replace("/", ".")
    row.headers.find(_.contains(csvHeader))
      .map(matchedHeader => row.headers.indexOf(matchedHeader))
      .fold [JsResult[JsValue]] {
      JsError(path -> 
        ValidationError("Could not find column " + csvHeader + ".",
          CSVErrorInfo(row.resourceIdentifier, row.rowNr)))
    } { startIndex =>
      val matchingHeaders = row.headers.drop(startIndex).takeWhile { _.startsWith(csvHeader) }
      val strList = CSVColumnUtil.getColumnRange(matchingHeaders)(row)
      val qbType = schema.items

      val childElements = (0 until strList.size).map {
        idx => convert (qbType, path(idx), Seq.empty)
      }

      if (! childElements.exists (_.asOpt.isEmpty) ) {
        JsSuccess(JsArray (childElements.collect {
          case (JsSuccess (s, p) ) => s
        }))
      } else {
        JsError(childElements.collect{
          case JsError(err) => err }.reduceLeft(_ ++ _))
      }
    }

  }


  def pathExists(path: JsPath)(implicit root: CSVRow): Boolean = Try {
    getColumnData(path)(filter(path))
  }.map(_ != "").getOrElse(false)

  def filter[A](path: String)(implicit row: CSVRow): CSVRow = row

  implicit def toCSVPath(path: JsPath): String = resolvePath(path)

  // TODO: only called atPrimitive, could be inlined
  // note this conforms with default split strategy
  def resolvePath(path: JsPath): String = {
    path.path.foldLeft("")((stringPath, nextNode) => {
      nextNode match {
        case k: KeyPathNode => if (stringPath == "") k.key else stringPath + "." + k.key
        case idx: IdxPathNode => s"$stringPath[${idx.idx}]"
        case _ => ""
      }
    })
  }
}
