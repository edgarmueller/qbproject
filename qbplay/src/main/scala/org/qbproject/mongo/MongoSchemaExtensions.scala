package org.qbproject.mongo

import org.qbproject.schema._
import reactivemongo.bson.BSONObjectID
import play.api.libs.json._
import org.qbproject.schema.{FormatRule, ValidationRule, QBStringImpl, QBClass}

object MongoSchemaExtensions {

  class QBObjectId(rules: Set[ValidationRule[JsString]]) extends QBStringImpl(rules) {
    override def toString = "objectId"
  }

  def objectId = new QBObjectId(Set(new ObjectIdRule))

  class ObjectIdRule extends FormatRule[JsString] {
    val format = "objectId"

    def isValid(str: JsString): Boolean = BSONObjectID.parse(str.value).isSuccess
  }

  def read(schema: QBClass)(instance: JsObject): JsResult[JsObject] = {
    new MongoTransformer(schema).read(instance)
  }

  def write(schema: QBClass)(instance: JsObject): JsResult[JsObject] = {
    new MongoTransformer(schema).write(instance)
  }
}