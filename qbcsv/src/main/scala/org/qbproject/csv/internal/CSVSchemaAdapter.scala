package org.qbproject.csv.internal

import org.qbproject.csv.internal.CSVColumnUtil._
import org.qbproject.schema._
import org.qbproject.schema.internal.QBAdapter
import play.api.data.validation.ValidationError
import play.api.libs.json._

import scala.util.Try
import scalaz.Validation.fromTryCatch

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
        case bool: QBBoolean => JsBoolean(asBoolean(path))
        case int: QBInteger => JsNumber(asDouble(path))
        case num: QBNumber => JsNumber(asDouble(path))
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
    annotations.collectFirst {
      case optional: QBOptionalAnnotation =>
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
    val csvHeader = resolvePath(path)
    row.headers.find(_.contains(csvHeader))
      .map(matchedHeader => row.headers.indexOf(matchedHeader))
      .fold[JsResult[JsValue]] {
      JsError(path ->
        ValidationError("Could not find column " + csvHeader + ".",
          CSVErrorInfo(row.resourceIdentifier, row.rowNr + 2)))
    } { startIndex =>
      val matchingHeaders = row.headers.drop(startIndex).takeWhile {
        _.startsWith(csvHeader)
      }
      val qbType = schema.items

      val div = qbType match {
        case qbClass: QBClass => qbClass.attributes.size // TODO: what about optionals?
        case _ => 1
      }

      val matchedValues = (for {
        group <- CSVColumnUtil.getColumnRange(matchingHeaders)(row).grouped(div).toList
        if group.exists(!_.trim.isEmpty)
      } yield {
        group
      }).flatten

      val childElements = (0 until matchedValues.size / div).map {
        idx => convert(qbType, path(idx), Seq.empty)
      }

      if (!childElements.exists(_.asOpt.isEmpty)) {
        JsSuccess(JsArray(childElements.collect {
          case (JsSuccess(s, p)) => s
        }))
      } else {
        JsError(childElements.collect {
          case JsError(err) => err
        }.reduceLeft(_ ++ _))
      }
    }

  }

  def pathExists(path: JsPath)(implicit root: CSVRow): Boolean = Try {
    getColumnData(path)
  }.map(_ != "").getOrElse(false)

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
