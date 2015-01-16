package org.qbproject.mongo

import org.qbproject.mongo.MongoOp.MongoOp
import org.qbproject.schema._
import org.qbproject.schema.QBSchema._
import org.qbproject.mongo.MongoConversion._
import org.qbproject.schema.internal.json.JsValueUpdateBuilder
import play.api.libs.json.Json.JsValueWrapper
import play.api.libs.json._

object DefaultMongoConversion {

  def apply: QBClass => MongoConversion = schema => new MongoConversion {

    val extendedQBKeys: Map[Class[_ <: QBType], String] = Map(
      classOf[QBObjectId] -> "$oid",
      classOf[QBDateTime] -> "$date",
      classOf[QBPosixTime] -> "$date"
    )

    val extendedQBSchema: QBClass = {

      def wrapWithClass(attrName: String): QBType => QBClass = qbType => qbClass(List(attrName -> qbType))

      extendedQBKeys.foldLeft(schema)((s, key) =>
        s.updateByPredicate(key._1.isInstance, wrapWithClass(key._2))
      )
    }

    val fromExtendedJson: JsObject => JsResult[JsObject] = { (o: JsObject) =>
      val builder = new JsValueUpdateBuilder(extendedQBSchema)
      val result = extendedQBKeys.foldLeft(builder)((builder, key) =>
        builder.byTypeAndPredicate[QBClass](_.hasAttribute(key._2) ) {
          case obj: JsObject => obj \ key._2
        }
      ).go(o)
      JsSuccess(result)
    }

    val toExtendedJson: JsObject => JsResult[JsObject] = { (o: JsObject) =>

      def wrapWithJsObject(fieldName: String, value: JsValueWrapper) = Json.obj(fieldName -> value)
      val result = extendedQBKeys.foldLeft(new JsValueUpdateBuilder(schema))((builder, key) =>
        builder.byPredicate(key._1.isInstance) {
          case s: JsString  => wrapWithJsObject(key._2, s.value)
          case n: JsNumber  => wrapWithJsObject(key._2, n.value)
          case b: JsBoolean => wrapWithJsObject(key._2, b.value)
        }
      ).go(o)
      // TODO: add logger
      JsSuccess(result)
    }

    def toMongoJson(op: MongoOp): InBound = toExtendedJson

    def fromMongoJson(op: MongoOp): OutBound = fromExtendedJson

  }
}
