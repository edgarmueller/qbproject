package org.qbproject.performance

import play.api.libs.json._
import org.qbproject.schema.internal.json.JsVisitor
import org.qbproject.schema.internal.visitor.QBPath
import org.qbproject.schema.{QBArray, QBClass, QBPrimitiveType}

trait JsIdentityVisitor extends JsVisitor {

  def atPrimitive[A <: JsValue](schema: QBPrimitiveType[A], jsValue: A, path: QBPath): JsResult[JsValue] = {
    JsSuccess(jsValue)
  }

  def atArray(schema: QBArray, elements: Seq[JsValue], path: QBPath, arr: JsArray): JsResult[JsArray] = {
    JsSuccess(arr)
  }

  def atObject(schema: QBClass, fields: Seq[(String, JsValue)], path: QBPath, jsObject: JsObject): JsResult[JsObject] = {
    JsSuccess(jsObject)
  }
}
