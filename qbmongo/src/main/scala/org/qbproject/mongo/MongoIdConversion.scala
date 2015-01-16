package org.qbproject.mongo

import org.qbproject.mongo.MongoConversion.{OutBound, InBound}
import org.qbproject.mongo.MongoOp.MongoOp
import org.qbproject.schema.QBClass
import play.api.libs.json._

object MongoIdConversion {

  def apply: QBClass => MongoConversion = _ => new MongoConversion {

    override def toMongoJson(op: MongoOp): InBound = toMongoId
    override def fromMongoJson(op: MongoOp): OutBound = fromMongoId

    object toMongoId extends (JsObject => JsResult[JsObject]) {
      override def apply(jsObject: JsObject): JsResult[JsObject]= JsSuccess(JsObject(jsObject.fields.map {
        case ("id", value) => ("_id", value)
        case fd => fd
      }))
    }

    object fromMongoId extends (JsObject => JsResult[JsObject]) {
      override def apply(jsObject: JsObject): JsResult[JsObject] = JsSuccess(
        JsObject(jsObject.fields.map {
          case ("_id", value) => ("id", value)
          case fd => fd
        }))
    }
  }

}
