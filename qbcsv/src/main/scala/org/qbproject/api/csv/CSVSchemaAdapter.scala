package org.qbproject.api.csv

import play.api.libs.json._
import org.qbproject.schema._
import scala.collection.immutable.Stream
import scalaz.Validation
import scalaz.Validation.fromTryCatch
import scala.util.Try
import org.qbproject.api.csv.CSVColumnUtil._
import org.qbproject.schema.internal.QBAdapter

trait CSVSchemaAdapter extends QBAdapter[CSVRow] {

  override def atPrimitive[A <: QBPrimitiveType[_]](schema: A, path: JsPath)(implicit root: CSVRow): JsResult[JsValue] = {
    fromTryCatch({
      schema match {
        case str: QBString =>
          val str = asString(path)(filter(path)(root))
          if (str.length == 0 && schema.rules.exists(_.isInstanceOf[EnumRule])) {
            // do not accept empty string if schema is an enum
            JsUndefined("")
          } else {
            JsString(str)
          }
        case bool: QBBoolean => JsBoolean(asBoolean(path)(filter(path)(root)))
        case int: QBInteger => JsNumber(asDouble(path)(filter(path)(root)))
        case num: QBNumber => JsNumber(asDouble(path)(filter(path)(root)))
      }
    }).fold(t => JsError(path, t.getMessage), JsSuccess(_))
  }

  override def atArray(schema: QBArray, path: JsPath)(implicit root: CSVRow): JsResult[JsValue] = {
    resolveArray(path)(root).fold(t => JsError(path, t.getMessage), size => {
      val childElements = (0 until size).map {
        idx => convert(schema.items, path(idx))
      }
      if (!childElements.exists(_.asOpt.isEmpty)) {
        JsSuccess(JsArray(childElements.collect {
          case (JsSuccess(s, p)) => s
        }), path)
      } else {
        JsError(childElements.collect {
          case JsError(e) => e
        }.reduceLeft(_ ++ _))
      }
    })
  }

  /**
   * Called when the schema visitor encounters an object.
   * Note that fields has type (String, Seq[O]) since an object is a composite type.
   */
  //  def atObject(schema: QBObject, path: JsPath): JsResult[JsValue] = {
  //    JsSuccess(JsObject(fields))
  //  }

  // ---

  def pathExists(path: JsPath)(implicit root: CSVRow): Boolean = Try {
    getColumnData(path)(filter(path))
  }.map(_ != "").getOrElse(false)

  protected def resolveArray(path: JsPath)(implicit row: CSVRow): Validation[Throwable, Int] = {
    fromTryCatch {
      val pathToArray = resolvePath(path)
      Stream
        .from(0)
        .takeWhile {
        idx =>
          !row.headers
            .filter(columnName => columnName.startsWith(s"$pathToArray[$idx]"))
            .forall(isColumnEmpty)
      }
        .size
    }
  }

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

//trait CSVTransformer extends SchemaVisitor[CSVRow] {
//  def validate(schema: QBValue)(implicit row: CSVRow) = this.visit(schema)(row)
//}

//object CSVTransformer extends CSVTransformer {
//  val behavior = new CSVSchemaAdapter {}
//}

class CSVAdaptedValidator(rewrites: List[(String, String, String => String)] = Nil) extends CSVAdapter(rewrites)

/**
 * TODO Refactor this shit
 */
class CSVAdapter(rewrites: List[(String, String, String => String)]) extends CSVSchemaAdapter {

  override def filter[A](path: String)(implicit row: CSVRow): CSVRow = {
    rewrites.find(_._1 == path)
      .map { filter =>
      val (path, adaptedPath, filterFn) = filter
      new CSVRow(row.row, row.headers) {
        override def getColumnData(colName: String): String = {
          if (colName == path) {
            filterFn(super.getColumnData(adaptedPath))
          } else {
            super.getColumnData(path)
          }
        }
      }
    }.getOrElse(row)
  }

  override def resolveArray(path: JsPath)(implicit row: CSVRow): Validation[Throwable, Int] = {
    rewrites.find(_._1 == resolvePath(path)).fold(super.resolveArray(path))(filter =>
      fromTryCatch {
        getColumnDataAsList(filter._2).size
      })
  }
}
