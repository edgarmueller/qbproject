package org.qbproject.schema.internal.visitor

import play.api.libs.json._
import org.qbproject.schema.internal._
import org.qbproject.schema.{QBArray, QBClass, QBPrimitiveType}

/**
 * May be mixed into the JsValueProcessor and defines the actual behavior
 * performed by the value processor when encountering a node of the given type.
 *
 * @tparam O
 *           the output type
 */
trait Visitor[O] {

  /**
   * Called when the value processor encounters a primitive value, that is, a number, a string or a boolean.
   *
   * @param schema
   *             the schema of a primitive type
   * @param jsValue
   *             the matched primitive JsValue
   * @param path
   *             the current path
   * @tparam A
   *             the actual primitive type which must be a subtype of JsValue
   * @return a JsResult containing a result of type O
   */
  def atPrimitive[A <: JsValue](schema: QBPrimitiveType[A], jsValue: A, path: QBPath): JsResult[O]

  /**
   * Called when the value processor encounters an array.
   * Note that elements has type Seq[O] since an array is a composite type.
   *
   * @param schema
   *               the array schema
   * @param elements
   *               the computed result for each element of the array
   * @param path
   *               the current path
   * @param jsArray
   *               the matched array
   * @return a JsResult containing a result of type O
   */
  def atArray(schema: QBArray, elements: List[O], path: QBPath, jsArray: JsArray): JsResult[O]

  /**
   * Called when the value processor encounters an object.
   * Note that fields has type (String, Seq[O]) since an object is a composite type.
   *
   * @param schema
   *             the object schema
   * @param fields
   *             the computed result for each field of the object
   * @param path
   *             the current path
   * @param jsObject
   *             the matched object
   * @return a JsResult containing a result of type O
   */
  def atObject(schema: QBClass, fields: List[(String, O)], path: QBPath, jsObject: JsObject): JsResult[O]

}




