package org.qbproject.mongo

import org.qbproject.schema._
import org.qbproject.schema.QBSchema._
import org.qbproject.schema.internal.json.mapper.JsTypeMapperBuilder
import org.qbproject.mongo.MongoSchemaExtensions._
import play.api.libs.json._
import scala.Some

class MongoTransformer(schema: QBClass) {

  val updatedSchema       = updateSchema(schema)
  val readMappingBuilder  = readMappingBuilderFor(updatedSchema)
  val writeMappingBuilder = writeMappingBuilderFor(schema)

  private def updateSchema(schema: QBClass): QBClass = {
    schema
      .map[QBObjectId](attr => qbClass("$oid" -> objectId))
      .map[QBDateTime](attr => qbClass("$date" -> qbDateTime))
      .map[QBPosixTime](attr => qbClass("$date" -> qbPosixTime))
  }

  private def readMappingBuilderFor(schema: QBClass): JsTypeMapperBuilder = {
    new JsTypeMapperBuilder(schema).map( qbType =>
      qbType.isInstanceOf[QBClass] && (qbType.asInstanceOf[QBClass].attributes.exists(_.name == "$oid")
        || qbType.asInstanceOf[QBClass].attributes.exists(_.name == "$date"))
    ) {
      case o: JsObject if o.fieldSet.exists(_._1 == "$oid") => o.fieldSet.find(_._1 == "$oid").get._2
      case o: JsObject if o.fieldSet.exists(_._1 == "$date")=> o.fieldSet.find(_._1 == "$date").get._2
      case o: JsObject if o.fieldSet.exists(_._1 == "$date") => o.fieldSet.find(_._1 == "$date").get._2
      case j => j
    }
  }

  private def writeMappingBuilderFor(schema: QBClass): JsTypeMapperBuilder = {
    new JsTypeMapperBuilder(schema).map[QBObjectId] {
      case JsString(id) => Json.obj("$oid" -> id)
      case j => j
    }.map[QBDateTime] {
      case JsString(date) => Json.obj("$date" -> date)
      case j => j
    }.map[QBPosixTime] {
      case JsNumber(time) => Json.obj("$date" -> time)
      case j => j
    }
  }

  // ugly move
  private def idTransRead(jsObject: JsObject) = {
    JsObject(jsObject.fields.map {
      case ("_id", value) => ("id", value)
      case fd => fd
    })
  }

  private def idTransWrite(jsObject: JsObject) = {
    JsObject(jsObject.fields.map {
      case ("id", value) => ("_id", value)
      case fd => fd
    })
  }

  // TODO rename me (e.g. readFromMongo)
  def read(
      jsObj: JsObject, 
      otherSchema: Option[QBClass] = None,
      otherValidator: Option[QBValidator] = None): JsResult[JsObject] = {
    // TODO: handle error cases where field can not be found
    val mappingBuilder = otherSchema match {
      case None    => this.readMappingBuilder
      case Some(s) => readMappingBuilderFor(updateSchema(s))
    }

    val updated = mappingBuilder.go(idTransRead(jsObj))
    otherValidator.getOrElse(QBValidator)
      .validate(otherSchema.getOrElse(schema))(updated)
  }

  // TODO rename me (e.g. writeToMongo)
  def write(
      obj: JsObject, 
      otherSchema: Option[QBClass] = None,
      otherValidator: Option[QBValidator] = None): JsResult[JsObject] = {

    val mappingBuilder = otherSchema match {
      case None    => this.writeMappingBuilder
      case Some(s) => writeMappingBuilderFor(s)
    }
    // TODO: avoid cast
    otherValidator.getOrElse(QBValidator)
      .validate(otherSchema.getOrElse(schema))(obj).map(js => idTransWrite(mappingBuilder.go(js)))
  }
}