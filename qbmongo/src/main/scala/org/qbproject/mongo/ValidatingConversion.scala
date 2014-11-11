package org.qbproject.mongo

import org.qbproject.mongo.MongoConversion.{InBound, OutBound}
import org.qbproject.mongo.MongoOp.MongoOp
import org.qbproject.schema._
import org.qbproject.schema.QBSchema._

object ValidatingConversion {

  def apply: QBClass => MongoConversion = schema => new MongoConversion {

    override def toMongoJson(op: MongoOp): InBound = op match {
      case MongoOp.Create => QBValidator.validate(schema ? "id")
      case _ =>  QBPartialValidator.validate(schema)
    }

    override def fromMongoJson(op: MongoOp): OutBound = QBValidator.validate(schema)
  }

}
