package org.qbproject.schema

import play.api.libs.json._
import play.api.libs.json.JsObject
import org.qbproject.schema.internal.visitor.QBPath
import org.qbproject.schema.internal.json._
import org.qbproject.schema.internal.json.serialization.JSONSchemaReads
import org.qbproject.schema.internal.json.serialization.JSONSchemaAnnotationWrites
import org.qbproject.schema.internal.json.processors.JsDefaultValueProcessor

object QBSchema
  extends QBSchemaOps 
  with QBSchemaDSL
  with JSONSchemaReads 
  with JSONSchemaAnnotationWrites
  with JsValueUpdateOps

trait QBValidator  {

  val validationInstance = JsValidationVisitor()
  val processor = JsDefaultValueProcessor()

  def validate[J <: JsValue](schema: QBType)(input: J): JsResult[J] =
    processor.process(schema, QBPath(), input, validationInstance).asInstanceOf[JsResult[J]]

}

trait PartialValidator extends QBValidator{
  override val processor = new JsDefaultValueProcessor {
    override def ignoreMissingFields = true
  }
}

object QBValidator extends QBValidator
object QBPartialValidator extends PartialValidator

case class QBJson(json: JsObject, schema: QBClass)