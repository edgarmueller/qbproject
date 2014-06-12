package org.qbproject.schema

import org.qbproject.api.schema._
import play.api.libs.json._

trait QBAdapter[I] {

  type PathBuilder = I => JsValue

  def adapt(schema: QBClass)(root: I): JsResult[JsValue] = atObject(schema, JsPath())(root)

  def pathBuilders: Map[String, PathBuilder] = Map.empty

  def convert(qbType: QBType, path: JsPath)(implicit root: I): JsResult[JsValue] = {
    val stringPath = stripOffSlash(path)
    pathBuilders.get(stringPath).fold {
      qbType match {
        case arr: QBArray => atArray(arr, path)
        case obj: QBClass => atObject(obj, path)
        case schema: QBPrimitiveType[_] => atPrimitive(schema, path)
      }
    } {
      builder => JsSuccess(builder(root))
    }
  }

  def atObject[A](schema: QBType, path: JsPath)(implicit root: I): JsResult[JsValue] = {
    schema match {
      case obj: QBClass =>
        val fields = obj.attributes.map(fd => fd.name -> convert(fd.qbType, path \ fd.name))
        JsSuccess(JsObject(fields.collect {
          case (fieldName, JsSuccess(res, _)) if !res.isInstanceOf[JsUndefined] =>
            (fieldName, res)
        }))
      case q => convert(q, path)
    }
  }

  def atPrimitive[A <: QBPrimitiveType[_]](schema: A, path: JsPath)(implicit root: I): JsResult[JsValue]

  def atArray(schema: QBArray, path: JsPath)(implicit root: I): JsResult[JsValue]

  private def stripOffSlash(path: JsPath): String = path.toString().substring(1)

}
