package org.qbproject.schema.internal

import org.qbproject.schema._
import play.api.libs.json._

trait QBAdapter[I] {

  type PathBuilder = I => JsValue

  def adapt(schema: QBClass)(root: I): JsResult[JsValue] = atObject(schema, JsPath(), Seq.empty)(root)

  def pathBuilders: Map[String, PathBuilder] = Map.empty

  def convert(qbType: QBType, path: JsPath, annotations: Seq[QBAnnotation])(implicit root: I): JsResult[JsValue] = {
    pathBuilders.get(resolvePath(path)).fold {
      qbType match {
        case arr: QBArray => atArray(arr, path, annotations)
        case obj: QBClass => atObject(obj, path, annotations)
        case schema: QBPrimitiveType[_] => atPrimitive(schema, path, annotations)
      }
    } {
      builder => JsSuccess(builder(root))
    }
  }

  def atObject[A](schema: QBType, path: JsPath, annotations: Seq[QBAnnotation])(implicit root: I): JsResult[JsValue] = {
    schema match {
      case obj: QBClass =>
        val fields = obj.attributes.map(attr => attr.name -> convert(attr.qbType, path \ attr.name, attr.annotations))

        // TODO: duplicate code, we have this somewhere in the core, too
        if (fields.exists(_._2.asOpt.isEmpty)) {
          JsError(fields.collect { case (p, JsError(err)) => err }.reduceLeft(_ ++ _))
        } else {
          JsSuccess(JsObject(fields.collect {
            case (fieldName, JsSuccess(res, _)) if !res.isInstanceOf[JsUndefined] =>
              (fieldName, res)
          }))
        }
      case q => convert(q, path, Seq.empty)
    }
  }

  def atPrimitive[A <: QBPrimitiveType[_]](schema: A, path: JsPath, annotations: Seq[QBAnnotation])(implicit root: I): JsResult[JsValue]

  def atArray(schema: QBArray, path: JsPath, annotations: Seq[QBAnnotation])(implicit root: I): JsResult[JsValue]

  // note this conforms with default split strategy
  def resolvePath(path: JsPath): String

}
