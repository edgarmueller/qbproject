package org.qbproject.mongo

import org.qbproject.mongo.MongoConversion.{InBound, OutBound}
import org.qbproject.mongo.MongoOp.MongoOp
import play.api.libs.json.{JsResult, JsObject}

trait MongoConversion {

  def toMongoJson(op: MongoOp = MongoOp.Null): InBound
  def toMongoJson: InBound = toMongoJson(MongoOp.Null)
  def fromMongoJson(op: MongoOp = MongoOp.Null): OutBound
  def fromMongoJson: OutBound = fromMongoJson(MongoOp.Null)
}

object MongoConversion {

  type InBound = JsObject => JsResult[JsObject]
  type OutBound = JsObject => JsResult[JsObject]

  implicit class MongoJsonAdapterOps(adapter: MongoConversion) {
    def compose(otherAdapter: MongoConversion) = new MongoConversion {

      override def toMongoJson(op: MongoOp): InBound = (o: JsObject) => otherAdapter.toMongoJson(op)(o).flatMap(adapter.toMongoJson(op))

      override def fromMongoJson(op: MongoOp): OutBound = (o: JsObject) => adapter.fromMongoJson(op)(o).flatMap(otherAdapter.fromMongoJson(op))
    }
  }
}