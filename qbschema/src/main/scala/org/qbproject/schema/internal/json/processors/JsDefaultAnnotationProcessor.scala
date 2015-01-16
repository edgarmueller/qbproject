package org.qbproject.schema.internal.json.processors

import org.qbproject.schema.internal._
import org.qbproject.schema.internal.visitor._
import play.api.libs.json._
import org.qbproject.schema.{QBDefaultAnnotation, QBAttribute}

/**
 * Handles default value annotations.
 */
class JsDefaultAnnotationProcessor extends AnnotationProcessor {

  /**
   * @inheritdoc
   *
   * @param attr
   *             the current attribute in scope
   * @param input
   *             the attribute value
   * @param path
   *             the current path
   * @param jsObject
   *             the parent JsObject
   * @return a JsResult containing a result of type O
   */
  def process(attr: QBAttribute, input: Option[JsValue], path: QBPath, jsObject: JsObject): Option[JsValue] = {
    if (input.isDefined) {
      input
    } else {
      attr.annotations
        .collectFirst { case default: QBDefaultAnnotation => default}
        .fold[Option[JsValue]] {
        None
      } { defaultAnnotation =>
        Some(defaultAnnotation.value)
      }
    }
  }

}