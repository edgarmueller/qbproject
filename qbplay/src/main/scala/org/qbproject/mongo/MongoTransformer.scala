package org.qbproject.mongo

import org.qbproject.schema._
import org.qbproject.schema.QBSchema._
import org.qbproject.schema.internal.json.mapper.JsValueUpdateBuilder
import play.api.libs.json._

class MongoTransformer(schema: QBClass,
                       postWriteUpdates: List[JsObject => JsObject] = List(toMongoId),
                       postReadUpdates: List[JsObject => JsObject] = List(fromMongoId)) {

  def extendedJsonKeys: List[(Class[_ <: QBType], String)] = List(
    (classOf[QBObjectId], "$oid"),
    (classOf[QBDateTime], "$date"),
    (classOf[QBPosixTime], "$date")
  )

  val extJsonSchema       = toExtendedJsonSchema(schema)
  val fromExtJsonBuilder  = fromExtendedJson(extJsonSchema)
  val toExtJsonBuilder    = toExtendedJson(schema)

  protected def toExtendedJsonSchema(schema: QBClass): QBClass = {
    extendedJsonKeys.foldLeft(schema)((s, x) =>
      s.updateByPredicate(a => x._1.isInstance(a), attr => qbClass(x._2 -> attr))
    )
  }

  protected def fromExtendedJson(schema: QBClass): JsValueUpdateBuilder = {
    extendedJsonKeys.foldLeft(new JsValueUpdateBuilder(schema))((builder, key) =>
      builder.byTypeAndPredicate[QBClass](_.hasAttribute(key._2)) {
        case obj: JsObject => obj.fieldSet.find(_._1 == key._2).get._2
      }
    )
  }

  protected def toExtendedJson(schema: QBClass): JsValueUpdateBuilder = {
    extendedJsonKeys.foldLeft(new JsValueUpdateBuilder(schema))((builder, key) =>
      builder.byPredicate(a => key._1.isInstance(a)) {
        case JsString(s) => Json.obj(key._2 -> s)
        case JsNumber(s) => Json.obj(key._2 -> s)
        case JsBoolean(s) => Json.obj(key._2 -> s)
      }
    )
  }

  def fromMongoJson(
      jsObj: JsObject, 
      otherSchema: Option[QBClass] = None,
      otherValidator: Option[QBValidator] = None): JsResult[JsObject] = {

    val mappingBuilder = otherSchema match {
      case None    => this.fromExtJsonBuilder
      case Some(s) => fromExtendedJson(toExtendedJsonSchema(s))
    }

    val updated = mappingBuilder.go(
      postReadUpdates.foldLeft(jsObj)((obj, converter) =>
        converter(obj)
      )
    )

    otherValidator.getOrElse(QBValidator)
      .validate(otherSchema.getOrElse(schema))(updated)
  }

  def toMongoJson(
      obj: JsObject, 
      otherSchema: Option[QBClass] = None,
      otherValidator: Option[QBValidator] = None): JsResult[JsObject] = {

    val jsValueUpdater = otherSchema match {
      case None    => this.toExtJsonBuilder
      case Some(s) => toExtendedJson(s)
    }

    val validatedResult = otherValidator
      .getOrElse(QBValidator)
      .validate(otherSchema.getOrElse(schema))(obj)

    validatedResult.map(res =>
      postWriteUpdates.foldLeft(jsValueUpdater.go(res))((r, converter) =>
        converter(r)
      )
    )
  }
}