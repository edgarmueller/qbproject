package org.qbproject.schema.internal.json.processors

import play.api.libs.json._
import org.qbproject.schema.internal._
import org.qbproject.schema.internal.visitor._
import org.qbproject.schema.internal.QBSchemaUtil._
import org.qbproject.schema.{QBOptionalAnnotation, QBAttribute}

/**
 * Handles optional annotations.
 */
class JsOptionalAnnotationProcessor extends AnnotationProcessor {

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
    val optionalAnnotation = attr.annotations.collectFirst { case optional: QBOptionalAnnotation => optional }
    optionalAnnotation.fold[Option[JsValue]](None)(annotation =>
      if (jsObject.keys.contains(attr.name) && input.isDefined && isNotNull(input.get)) {
        Some(input.get)
      } else {
        annotation.fallBack.fold[Option[JsValue]](None)(Some(_))
      })
  }
}