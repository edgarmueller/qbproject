package org.qbproject.csv

import play.api.libs.json._
import scalaz.Validation.fromTryCatch
import scala.util.Try
import org.qbproject.schema._
import org.qbproject.api.schema._
import org.qbproject.api.csv.{CSVColumnUtil, DefaultArrayPathBuilder}
import CSVColumnUtil._
import org.qbproject.api.csv.DefaultArrayPathBuilder

trait CSVSchemaAdapter extends QBAdapter[CSVRow] {

  override def atPrimitive[A <: QBPrimitiveType[_]](schema: A, path: JsPath, annotations: Seq[QBAnnotation])(implicit root: CSVRow): JsResult[JsValue] = {
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
        case bool: QBBoolean => JsBoolean(asBoolean(path))
        case int: QBInteger => JsNumber(asDouble(path))
        case num: QBNumber => JsNumber(asDouble(path))
      }
    }).fold(throwable => {
      // check optionals and defaults
      annotations.collectFirst { case optional: QBOptionalAnnotation =>
        optional.fallBack
      }.fold[JsResult[JsValue]] {
        annotations.collectFirst {
          case default: QBDefaultAnnotation => default
        }.fold[JsResult[JsValue]] {
          JsError(path, throwable.getMessage)
        } {
          default => JsSuccess(default.value)
        }
      } { possibleFallBack =>
        JsSuccess(possibleFallBack.fold[JsValue] {
          JsUndefined("")
        } {
          identity
        })
      }
    }, JsSuccess(_))
  }

  override def atArray(schema: QBArray, path: JsPath, annotations: Seq[QBAnnotation])(implicit root: CSVRow): JsResult[JsValue] = {
    val csvHeader = path.toString.substring(1).replace("/", ".")
    root.headers.find(_.contains(csvHeader))
      .map(matchedHeader => root.headers.indexOf(matchedHeader))
      .fold [JsResult[JsValue]] {
      JsError("Could not find column " + csvHeader + ".")
    } { startIndex =>
      val matchingHeaders = root.headers.drop(startIndex).takeWhile { _.startsWith(csvHeader) }
      val strList = CSVColumnUtil.getColumnRange(matchingHeaders)(root)
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
