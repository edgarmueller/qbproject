package org.qbproject.schema.internal.json

import org.qbproject.schema.internal.visitor._
import org.qbproject.schema.{QBArray, QBClass, QBPrimitiveType, QBType}
import play.api.libs.json._

/**
 * Visitor that finds all types and paths for which the matcher evaluates to true.
 *
 * The matcher must be implemented by clients.
 */
object JsFilterVisitor {

  def apply(matcher: QBType => Boolean) = new Visitor[Seq[(QBType, QBPath)]] {

    def atPrimitive[A <: JsValue](schema: QBPrimitiveType[A], jsValue: A,
                                  path: QBPath): JsResult[Seq[(QBType, QBPath)]] = {
      if (matcher(schema)) {
        JsSuccess(List(schema -> path))
      } else {
        JsSuccess(List.empty)
      }
    }

    def atArray(schema: QBArray, elements: List[Seq[(QBType, QBPath)]], path: QBPath,
                jsArray: JsArray): JsResult[Seq[(QBType, QBPath)]] = {
      if (matcher(schema.items)) {
        JsSuccess(List.fill(elements.size)(path)
          .zipWithIndex
          .map { case (p, idx) => p :+ QBIdxNode(idx) }
          .map(idxPath => (schema.items, idxPath)))
      } else {
        JsSuccess(elements.flatten)
      }
    }

    def atObject(schema: QBClass, fields: List[(String, Seq[(QBType, QBPath)])], path: QBPath,
                 jsObject: JsObject): JsResult[Seq[(QBType, QBPath)]] = {
      if (matcher(schema)) {
        JsSuccess(schema -> path :: fields.flatMap(_._2))
      } else {
        JsSuccess(fields.flatMap(_._2))
      }
    }
  }
}

