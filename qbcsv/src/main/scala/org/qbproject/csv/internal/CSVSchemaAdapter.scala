package org.qbproject.csv.internal

import java.util.logging.{Level, Logger}

import org.qbproject.csv.internal.CSVColumnUtil._
import org.qbproject.schema._
import org.qbproject.schema.internal.QBAdapter
import play.api.data.validation.ValidationError
import play.api.libs.json._

import scala.collection.immutable.IndexedSeq
import scala.util.Try

trait CSVSchemaAdapter extends QBAdapter[CSVRow] {

  override def atPrimitive[A <: QBPrimitiveType[_]](schema: A, path: JsPath, annotations: Seq[QBAnnotation])(implicit row: CSVRow): JsResult[JsValue] = {
    row.getColumnData(resolvePath(path)) match {
      case Some(data) =>
        val possibleValue: Option[JsValue] = adaptData(schema, data)
        possibleValue match {
          case Some(jsValue) => JsSuccess(jsValue) // got value, all fine
          case None =>
            if (data.isEmpty) {
              tryAnnotations(schema, path, annotations, row)
            } else {
              // data is not empty therefore there must be a problem with the data
              val msg = s"Expected: $schema at $path, but got '$data'."
              Logger.getLogger(getClass.getName).log(Level.SEVERE, msg)
              JsError(path -> ValidationError(msg, CSVErrorInfo(row.resourceIdentifier, row.rowNr)))
            }
        }
      case None => tryAnnotations(schema, path, annotations, row)
    }
  }

  /**
   * Try to get a JsResult by inspecting any annotations.
   */
  private def tryAnnotations[A <: QBPrimitiveType[_]](schema: A, path: JsPath, annotations: Seq[QBAnnotation], row: CSVRow): JsResult[JsValue] = {
    annotations match {
      case hasDefaultValue(default) => JsSuccess(default)
      case isOptionalValue(Some(default)) => JsSuccess(default)
      case isOptionalValue(None) => JsSuccess(JsUndefined(""))
      case _ =>
        val msg = s"Expected: $schema at $path but the column is missing in the CSV."
        Logger.getLogger(getClass.getName).log(Level.SEVERE, msg)
        JsError(path -> ValidationError(msg, CSVErrorInfo(row.resourceIdentifier, row.rowNr)))
    }
  }

  private def adaptData[A <: QBPrimitiveType[_]](schema: A, data: String): Option[JsValue with Product with Serializable] = {
    schema match {
      case str: QBString if data.isEmpty && schema.rules.exists(_.isInstanceOf[EnumRule]) => None // do not accept empty string if schema is an enum
      case str: QBString if data.isEmpty => None // we handle this case later
      case str: QBString => Some(JsString(data))
      case bool: QBBoolean => tryBoolean(data).map(JsBoolean)
      case int: QBInteger => tryDouble(data).map(JsNumber(_))
      case num: QBNumber => tryDouble(data).map(JsNumber(_))
    }
  }

  object hasDefaultValue {
    def unapply(annotations: Seq[QBAnnotation]): Option[JsValue] = {
      annotations.collectFirst {
        case default: QBDefaultAnnotation => default.value
      }
    }
  }

  object isOptionalValue {
    def unapply(annotations: Seq[QBAnnotation]): Option[Option[JsValue]] = {
      annotations.collectFirst {
        case optional: QBOptionalAnnotation => optional.fallBack
      }
    }
  }

  override def atArray(schema: QBArray, path: JsPath, annotations: Seq[QBAnnotation])(implicit row: CSVRow): JsResult[JsValue] = {
    val csvHeader = resolvePath(path)
    row.headers.find(_.contains(csvHeader))
      .map(matchedHeader => row.headers.indexOf(matchedHeader))
      .fold[JsResult[JsValue]] {
      annotations match {
        case isOptionalValue(Some(default)) => JsSuccess(default)
        case isOptionalValue(None) => JsSuccess(JsUndefined(""))
        case _ => JsError(path -> ValidationError(s"Could not find column $csvHeader.", CSVErrorInfo(row.resourceIdentifier, row.rowNr + 2)))
      }
    } { startIndex => // found index of header

      val matchingHeaders = row.headers.drop(startIndex).takeWhile(_.startsWith(csvHeader))
      val qbType = schema.items

      val denominator = qbType match {
        case qbClass: QBClass => qbClass.attributes.size // TODO: what about optionals?
        case _ => 1
      }

      val matchedValues: List[String] = CSVColumnUtil.getColumnRange(matchingHeaders)(row)
        .grouped(denominator)
        .filterNot(_.exists(_.isEmpty))
        .flatten.toList

      val childElements: IndexedSeq[JsResult[JsValue]] = (0 until matchedValues.size / denominator).map {
        idx => convert(qbType, path(idx), Seq.empty)
      }

      if (!childElements.exists(_.asOpt.isEmpty)) {
        JsSuccess(JsArray(childElements.collect { case (JsSuccess(s, p)) => s }))
      } else {
        JsError(childElements.collect { case JsError(err) => err }.reduceLeft(_ ++ _))
      }
    }
  }

  def pathExists(path: JsPath)(implicit root: CSVRow): Boolean = Try {
    getColumnData(path)
  }.map(_ != "").getOrElse(false)

  implicit def toCSVPath(path: JsPath): String = resolvePath(path)

  def resolvePath(path: JsPath): String = {
    path.path.foldLeft("")((stringPath, nextNode) => {
      nextNode match {
        case keyNode: KeyPathNode => if (stringPath == "") keyNode.key else stringPath + "." + keyNode.key
        case idxNode: IdxPathNode => s"$stringPath[${idxNode.idx}]"
        case _ => ""
      }
    })
  }
}
