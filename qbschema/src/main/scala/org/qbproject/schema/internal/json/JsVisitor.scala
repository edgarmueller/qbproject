package org.qbproject.schema.internal.json

import org.qbproject.schema.internal._
import org.qbproject.schema.internal.visitor._
import play.api.libs.json._
import org.qbproject.schema.{QBArray, QBClass, QBPrimitiveType}

/**
 * Visitor behavior that produces a JsValue.
 */
trait JsVisitor extends Visitor[JsValue] {

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
  def atPrimitive[A <: JsValue](schema: QBPrimitiveType[A], jsValue: A, path: QBPath): JsResult[JsValue]

  /**
   * Called when the visitor encounters an array.
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
  def atArray(schema: QBArray, elements: List[JsValue], path: QBPath, jsArray: JsArray): JsResult[JsArray]

  /**
   * Called when the visitor encounters an object.
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
  def atObject(schema: QBClass, fields: List[(String, JsValue)], path: QBPath, jsObject: JsObject): JsResult[JsObject]
}
