package org.qbproject.schema.internal.visitor

import org.qbproject.schema.internal._
import play.api.libs.json.{JsValue, JsObject}
import org.qbproject.schema.QBAttribute

/**
 * <p>
 * Annotation processor are called if the attribute in scope has annotations
 * that match the annotation for which this extension has been registered.
 * <p>
 *
 * <p>
 * Annotation processors need to return a JsValue, either by transforming
 * the current input or by providing a new value. Thus, annotation processor, just
 * like type processors, can be thought of means to transform the input before the JsValueProcessor
 * executes the Visitor.
 * </p>
 *
 * <p>
 * In case annotation processors return None, the JsValueProcessor will simply ignore
 * the current attribute.
 * </p>
 */
trait AnnotationProcessor {

  /**
   * Execute this annotation processor.
   *
   * @param attr
   *             the current attribute in scope
   * @param input
   *             the attribute value
   * @param path
   *             the current path
   * @param jsObject
   *             the parent JsObject
   * @return an Option containing a result of type O. If None is returned
   *         the JsValueProcessor processing the attributes will ignore
   *         the current attribute
   */
  def process(attr: QBAttribute, input: Option[JsValue], path: QBPath, jsObject: JsObject): Option[JsValue]
}
