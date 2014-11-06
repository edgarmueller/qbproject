package org.qbproject.schema

import play.api.libs.json._
import play.api.libs.json.JsObject
import scala.reflect.ClassTag
import org.qbproject.schema.internal.visitor.QBPath
import org.qbproject.schema.internal.json._
import org.qbproject.schema.internal.json.serialization.JSONSchemaReads
import org.qbproject.schema.internal.json.serialization.JSONSchemaAnnotationWrites
import org.qbproject.schema.internal.json.processors.JsDefaultValueProcessor
import org.qbproject.schema.internal.json.mapper.{JsValueUpdateProcessor, JsValueUpdateOps}

object QBSchema
  extends QBSchemaOps 
  with QBSchemaDSL
  with JSONSchemaReads 
  with JSONSchemaAnnotationWrites
  with JsValueUpdateOps

trait QBValidator extends JsDefaultValueProcessor {

  val validationInstance = JsValidationVisitor()

  def validate[J <: JsValue](schema: QBType)(input: J): JsResult[J] =
    process(schema, QBPath(), input)(validationInstance).asInstanceOf[JsResult[J]]

}

trait PartialValidator { self: QBValidator =>
  override def ignoreMissingFields = true
}

object QBValidator extends QBValidator
object QBPartialValidator extends QBValidator with PartialValidator

case class QBJson(json: JsObject, schema: QBClass)

case class QBJsValueUpdater[A <: QBType : ClassTag]() extends JsValueUpdateProcessor[A]
