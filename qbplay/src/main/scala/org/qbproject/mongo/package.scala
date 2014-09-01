package org.qbproject

import org.qbproject.schema._
import play.api.libs.json._
import reactivemongo.bson.BSONObjectID

package object mongo {

  class QBObjectId(rules: Set[ValidationRule[JsString]]) extends QBStringImpl(rules) {
    override def toString = "objectId"
  }

  def objectId = new QBObjectId(Set(new ObjectIdRule))

  def objectId(endpoint: String) = new QBObjectId(Set(new ObjectIdRule, new KeyValueRule("endpoint", endpoint)))

  class ObjectIdRule extends FormatRule[JsString] {
    val format = "objectId"

    def isValid(str: JsString): Boolean = BSONObjectID.parse(str.value).isSuccess
  }

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
