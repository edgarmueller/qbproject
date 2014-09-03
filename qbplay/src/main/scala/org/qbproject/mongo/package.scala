package org.qbproject

import org.qbproject.schema._
import play.api.data.validation.ValidationError
import play.api.libs.json._
import reactivemongo.bson.BSONObjectID

import scalaz.{Failure, Success, Validation}

package object mongo {

  class QBObjectId(rules: Set[ValidationRule[JsString]]) extends QBStringImpl(rules) {
    override def toString = "objectId"
  }

  def objectId = new QBObjectId(Set(RegexRule("[0-9A-Fa-f]{24}")))

  def objectId(endpoint: String) = new QBObjectId(Set(RegexRule("[0-9A-F]{24}"), new KeyValueRule("endpoint", endpoint)))

  def read(schema: QBClass)(instance: JsObject): JsResult[JsObject] = {
    new MongoTransformer(schema).fromMongoJson(instance)
  }

  def write(schema: QBClass)(instance: JsObject): JsResult[JsObject] = {
    new MongoTransformer(schema).toMongoJson(instance)
  }

  object toMongoId extends (JsObject => JsObject) {
    override def apply(jsObject: JsObject): JsObject = JsObject(jsObject.fields.map {
      case ("id", value) => ("_id", value)
      case fd => fd
    })
  }

  object fromMongoId extends (JsObject => JsObject) {
    override def apply(jsObject: JsObject): JsObject = JsObject(jsObject.fields.map {
      case ("_id", value) => ("id", value)
      case fd => fd
    })
  }
}
