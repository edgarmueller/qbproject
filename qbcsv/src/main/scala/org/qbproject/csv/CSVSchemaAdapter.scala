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

  override def atPrimitive[A <: QBPrimitiveType[_]](schema: A, path: JsPath)(implicit root: CSVRow): JsResult[JsValue] = {
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
    }).fold(t => JsError(path, t.getMessage), JsSuccess(_))
  }

  override def atArray(schema: QBArray, path: JsPath)(implicit root: CSVRow): JsResult[JsValue] = {
    val stringPath = path.toString.substring(1)
    root.headers.find(_.contains(stringPath))
      .map(matchedHeader => root.headers.indexOf(matchedHeader))
      .fold [JsResult[JsValue]] {
      JsError("Could not find column " + stringPath + ".")
    } { startIndex =>
      val matchingHeaders = root.headers.drop(startIndex).takeWhile { _.startsWith(stringPath) }
      val strList = CSVColumnUtil.getColumnRange(matchingHeaders)(root)
      val qbType = schema.items
      //    qbType match {
      //      case _: QBPrimitiveType[_] =>
      val childElements = (0 until strList.size).map {
        idx => convert (qbType, path(idx))
      }

      if (! childElements.exists (_.asOpt.isEmpty) ) {
        JsSuccess(JsArray (childElements.collect {
          case (JsSuccess (s, p) ) => s
        }))
      } else {
        // TODO: clarify what to do in an error case
        JsError(childElements.collect{
          case JsError(err) => err }.reduceLeft(_ ++ _))
      }
      //      case _: QBClass =>
      // TODO: hack, we could infer the types here, actually, the CSV resolver should be able to handle this
      // with a schema
      //        JsSuccess(JsArray(strList.map(JsString(_))))
      //    }
    }

  }

  def pathExists(path: JsPath)(implicit root: CSVRow): Boolean = Try {
    getColumnData(path)(filter(path))
  }.map(_ != "").getOrElse(false)

  // ---

  def filter[A](path: String)(implicit row: CSVRow): CSVRow = row

  implicit def resolvePath(path: JsPath): String = {
    path.path.foldLeft("")((stringPath, nextNode) => {
      nextNode match {
        case k: KeyPathNode => if (stringPath == "") k.key else stringPath + "." + k.key
        case idx: IdxPathNode => s"$stringPath[${idx.idx}]"
        case _ => ""
      }
    })
  }
}
