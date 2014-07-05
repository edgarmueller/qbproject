package org.qbproject.schema.internal.json.serialization

import org.qbproject.schema.internal._
import play.api.libs.json._
import org.qbproject.schema._
import org.qbproject.schema.QBOptionalAnnotation
import play.api.libs.json.JsObject
import play.api.libs.json.JsString
import play.api.libs.json.JsArray
import org.qbproject.schema.QBDefaultAnnotation
import org.qbproject.schema.QBAttribute

trait JSONSchemaAnnotationWrites extends JSONSchemaWrites {

  val emptyJsObject = Json.obj()

  override def propertyExtensions = List(
    defaultExtension _,
    readOnlyExtension _)
  override def extensions = List(optionalExtension _)

  def defaultExtension(attr: QBAttribute): JsObject = {
    attr.annotations
      .collectFirst { case default: QBDefaultAnnotation => Json.obj("default" -> default.value) }
      .getOrElse(emptyJsObject)
  }

  def readOnlyExtension(attr: QBAttribute): JsObject = {
    attr.annotations
      .collectFirst { case _: QBReadOnlyAnnotation => Json.obj("readonly" -> "true") }
      .getOrElse(emptyJsObject)
  }

  def optionalExtension(obj: QBClass): JsObject = {
    val required = allNonOptionalAttributes(obj.attributes)
    Json.obj("required" -> JsArray(required))
  }

  def allNonOptionalAttributes(fields: Seq[QBAttribute]): Seq[JsString] =
    fields
      .filterNot(_.annotations.exists(_.isInstanceOf[QBOptionalAnnotation]))
      .map(_.name).map(JsString)
}

