package org.qbproject.schema.internal.visitor

import org.qbproject.schema.internal._
import play.api.libs.json.{JsResult, JsValue}
import org.qbproject.schema.QBType

/**
 * <p>
 * Type processor are called if the JsValueProcessor encounters a type for which a
 * type processor is registered.
 * <p>
 *
 * <p>
 * Type processors, just like annotation processors can be thought of means to transform
 * the input before the JsValueProcessor executes the Visitor, but must, in contrast to
 * annotation processors, always return a JsValue.
 * </p>
 */
trait TypeProcessor {

  /**
   * Execute this type processor.
   *
   * @param qbType
   *             the matched QB type
   * @param input
   *              the matched JsValue
   * @param path
   *             the current path
   * @return a JsResult containing a result of type JsValue
   */
  def process(qbType: QBType, input: JsValue, path: QBPath): JsResult[JsValue]
}