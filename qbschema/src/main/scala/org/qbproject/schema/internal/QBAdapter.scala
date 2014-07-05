package org.qbproject.schema.internal

import play.api.libs.json.{JsPath, JsValue, JsResult}
import org.qbproject.schema._

trait QBAdapter[I] {

  import QBSchema._

  def adapt(schema: QBClass)(root: I): JsResult[JsValue] = {
    schema.adapt {
      (p, q) => convert(q, p)(root)
    }
  }

  def convert(qbType: QBType, path: JsPath)(implicit root: I): JsResult[JsValue] = {
    qbType match {
      case arr: QBArray => atArray(arr, path)
      case obj: QBClass => adaptSchema(obj, path, (p, q) => convert(q, p)(root))
      case schema: QBPrimitiveType[_] => atPrimitive(schema, path)
    }
  }

  def atPrimitive[A <: QBPrimitiveType[_]](schema: A, path: JsPath)(implicit root: I): JsResult[JsValue]

  def atArray(schema: QBArray, path: JsPath)(implicit root: I): JsResult[JsValue]

}
