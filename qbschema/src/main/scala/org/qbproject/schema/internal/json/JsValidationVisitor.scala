package org.qbproject.schema.internal.json

import org.qbproject.schema.internal._
import org.qbproject.schema.internal.visitor.QBPath
import play.api.libs.json._
import org.qbproject.schema.{QBArray, QBClass, QBPrimitiveType}

/**
 * Validation behavior that calls the validates an instance against its schema.
 */
trait JsValidationVisitor extends JsVisitor {

  /**
   * @inheritdoc
   *
   * @param schema
   *             the schema of a primitive type
   * @param jsValue
   *             the matched primitive JsValue
   * @param path
   *             the current path
   * @tparam A
   *             the actual primitive type which must be a subtype of JsValue
   * @return a JsResult containing a JsValue result
   */
  def atPrimitive[A <: JsValue](schema: QBPrimitiveType[A], jsValue: A, path: QBPath): JsResult[JsValue] = {
    schema.validate(jsValue)
      .fold(
        JsError(path.toJsPath, _),
        JsSuccess(_)
      )
  }

  /**
   * @inheritdoc
   *
   * @param schema
   *               the array schema
   * @param elements
   *               the computed result for each element of the array
   * @param path
   *               the current path
   * @param jsArray
   *               the matched array
   * @return a JsResult containing a JsArray
   */
  def atArray(schema: QBArray, elements: Seq[JsValue], path: QBPath, jsArray: JsArray): JsResult[JsArray] = {
    val arr = JsArray(elements.toList)
    schema.validate(arr)
      .fold(
        JsError(path.toJsPath, _),
        JsSuccess(_)
      )
  }

  /**
   * @inheritdoc
   *
   * @param schema
   *             the object schema
   * @param fields
   *             the computed result for each field of the object
   * @param path
   *             the current path
   * @param jsObject
   *             the matched object
   * @return a JsResult containing a JsObject
   */
  def atObject(schema: QBClass, fields: Seq[(String, JsValue)], path: QBPath, jsObject: JsObject): JsResult[JsObject] = {
    val obj = JsObject(fields.toList)
    schema.validate(obj)
      .fold(
        JsError(path.toJsPath, _),
        JsSuccess(_)
      )
  }
}