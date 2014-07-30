package org.qbproject.schema.internal.json.mapper

import org.qbproject.schema.internal._
import org.qbproject.schema.internal.visitor._
import play.api.libs.json._
import org.qbproject.schema.{QBArray, QBClass, QBPrimitiveType, QBType}

/**
 * Visitor that finds all types and paths for which the matcher evaluates to true.
 *
 * The matcher must be implemented by clients.
 */
trait JsValueUpdateVisitor extends Visitor[Seq[(QBType, QBPath)]] {

  /**
   * Determines whether the visitor adds the type together with the matched path to the result set.
   *
   * @param qbType
   *             a qbType
   * @return true, if the QB type is of interest, false otherwise
   */
  def matcher(qbType: QBType): Boolean

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
   * @return a JsResult containing a sequence of all matched types and paths
   **/
  def atPrimitive[A <: JsValue](schema: QBPrimitiveType[A], jsValue: A,
                                path: QBPath): JsResult[Seq[(QBType, QBPath)]] = {
    if (matcher(schema)) {
      JsSuccess(List(schema -> path))
    } else {
      JsSuccess(List.empty)
    }
  }

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
   * @return a JsResult containing a sequence of all matched types and paths
   **/
  def atArray(schema: QBArray, elements: Seq[Seq[(QBType, QBPath)]], path: QBPath,
              jsArray: JsArray): JsResult[Seq[(QBType, QBPath)]] = {
    JsSuccess(if (matcher(schema.items)) {
      List.fill(elements.size)(path)
          .zipWithIndex
          .map { case (p, idx) => p.append(QBIdxNode(idx)) }
          .map(idxPath => (schema.items, idxPath))// ++ elements.flatten
    } else {
      elements.flatten
    })
  }

  /**
   * Called when the value processor encounters an object.
   *
   * @param schema
   *             the object schema
   * @param fields
   *             the computed result for each field of the object
   * @param path
   *             the matched path
   * @param jsObject
   *             the matched object
   * @return a JsResult containing a sequence of all matched types and paths
   */
  def atObject(schema: QBClass, fields: Seq[(String, Seq[(QBType, QBPath)])], path: QBPath,
               jsObject: JsObject): JsResult[Seq[(QBType, QBPath)]] = {
    if (matcher(schema)) {
     JsSuccess(List(schema -> path) ++ fields.flatMap(_._2))
    } else {
      JsSuccess(fields.flatMap(_._2))
    }
  }

}
