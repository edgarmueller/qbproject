package org.qbproject.csv.internal

import java.util.logging.{Level, Logger}

import org.qbproject.csv.internal.CSVColumnUtil._
import org.qbproject.schema._
import org.qbproject.schema.internal.QBAdapter
import play.api.data.validation.ValidationError
import play.api.libs.json._

import scala.util.Try

trait CSVSchemaAdapter extends QBAdapter[CSVRow] {

  override def atPrimitive[A <: QBPrimitiveType[_]](schema: A, path: JsPath, annotations: Seq[QBAnnotation])(implicit row: CSVRow): JsResult[JsValue] = {
    row.getColumnData(resolvePath(path)) match {
      case Some(data) =>
        val v: Option[JsValue] = schema match {
          case str: QBString if data.isEmpty && schema.rules.exists(_.isInstanceOf[EnumRule]) => None // do not accept empty string if schema is an enum
          case str: QBString if data.isEmpty => None // we handle this case later
          case str: QBString => Some(JsString(data))
          case bool: QBBoolean => tryBoolean(data).map(JsBoolean)
          case int: QBInteger => tryDouble(data).map(JsNumber(_))
          case num: QBNumber => tryDouble(data).map(JsNumber(_))
        }
        v match {
          case Some(jsValue) => JsSuccess(jsValue)
          case None =>
            if (data.isEmpty) {
              annotations match {
                case hasDefaultValue(default) => JsSuccess(default)
                case isOptionalValue(Some(default)) => JsSuccess(default)
                case isOptionalValue(None) => JsSuccess(JsUndefined(""))
                case _ if schema.isInstanceOf[QBString] => JsSuccess(JsString(data))
                case _ =>
                  val msg = "Expected: " + schema + " at " + path + " got empty String"
                  Logger.getLogger(getClass.getName).log(Level.SEVERE, msg)
                  JsError(path -> ValidationError(msg, CSVErrorInfo(row.resourceIdentifier, row.rowNr)))
              }
            } else {
              // data is not empty therefor there must be a problem with the data
              val msg = "Expected: " + schema + " at " + path + " got \"" + data + "\""
              Logger.getLogger(getClass.getName).log(Level.SEVERE, msg)
              JsError(path -> ValidationError(msg, CSVErrorInfo(row.resourceIdentifier, row.rowNr)))
            }
        }
      case None =>
        annotations match {
          case hasDefaultValue(default) => JsSuccess(default)
          case isOptionalValue(Some(default)) => JsSuccess(default)
          case isOptionalValue(None) => JsSuccess(JsUndefined(""))
          case _ =>
            val msg = "Expected: " + schema + " at " + path + " but the column is missing in the CSV."
            Logger.getLogger(getClass.getName).log(Level.SEVERE, msg)
            JsError(path -> ValidationError(msg, CSVErrorInfo(row.resourceIdentifier, row.rowNr)))
        }
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
      JsError(path -> ValidationError("Could not find column " + csvHeader + ".", CSVErrorInfo(row.resourceIdentifier, row.rowNr + 2)))
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
