package org.qbproject.mongo

import org.qbproject.mongo.MongoConversion.{OutBound, InBound}
import org.qbproject.mongo.MongoOp.MongoOp
import org.qbproject.schema.QBClass

object MongoIdConversion {

  def apply: QBClass => MongoConversion = _ => new MongoConversion {

    override def toMongoJson(op: MongoOp): InBound = toMongoId
    override def fromMongoJson(op: MongoOp): OutBound = fromMongoId
  }

}
