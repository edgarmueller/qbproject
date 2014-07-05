package org.qbproject.mongo

import play.api.libs.concurrent.Execution.Implicits.defaultContext
import play.api.libs.json._
import play.modules.reactivemongo.json.collection.JSONCollection
import play.modules.reactivemongo.json.ImplicitBSONHandlers._

import reactivemongo.api._
import reactivemongo.bson._
import reactivemongo.core.commands._

import scala.concurrent.Future
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import play.api.libs.json._
import play.modules.reactivemongo.json.ImplicitBSONHandlers._


class QBMongoCollection(collectionName: String)(db: DB) {

  type ID = String

  private lazy val collection = db.collection[JSONCollection](collectionName)
  
  /** Perform a raw command on the underlying mongocollection */
  def rawCollection = collection

  def getCount = db.command(Count(collectionName))

  def getAll(skip: Int = 0, limit: Int = 100): Future[List[JsObject]] = find(Json.obj(), skip, limit)

  def findOne(query: JsObject): Future[Option[JsObject]] = {
    collection.find(query)
      .cursor[JsObject]
      .headOption
  }

  def find(query: JsObject, skip: Int = 0, limit: Int = 100): Future[List[JsObject]] = {
    val cursor = collection.find(query)
      .options(QueryOpts().skip(skip))
      .cursor[JsObject]

    if (limit == -1) {
      cursor.collect[List]()
    } else {
      cursor.collect[List](limit)
    }
  }

  def findAndModify(query: JsObject, modifier: JsObject, upsert: Boolean = false): Future[Option[JsObject]] = {
    db.command(FindAndModify(
      collection.name,
      query,
      Update(modifier, true),
      upsert
    )).map(_.map(bsonToJson))
  }

  def getById(id: ID): Future[Option[JsObject]] = {
    collection.find(Json.obj("_id" -> Json.obj("$oid" -> id))).cursor[JsObject].headOption
  }

  def create(obj: JsObject): Future[JsObject] = {
    val toCreate = obj \ "_id" match {
      case no: JsUndefined => obj ++ Json.obj("_id" -> Json.obj(
        "$oid" -> BSONObjectID.generate.stringify
      ))
      case _ => obj
    }
    //    println(Json.prettyPrint(toCreate))
    collection.insert(toCreate).map(lastError => 
      if (lastError.ok) toCreate 
      else throw new RuntimeException("oh noes."))
  }

  def update(id: ID, update: JsObject): Future[JsObject] = this.update(Json.obj("_id" -> Json.obj("$oid" -> id)), update)

  def update(query: JsObject, update: JsObject): Future[JsObject] = {
    val updateJson = update \ "$set" match {
      case un: JsUndefined => Json.obj("$set" -> update)
      case _ => update
    }
    this.findAndModify(query, updateJson).map {
      case Some(js) => js
      case None     => throw new RuntimeException("No Result found.")
    }
  }


  // ---

  implicit def jsonToBson(obj: JsObject): BSONDocument = JsObjectWriter.write(obj)
  implicit def bsonToJson(obj: BSONDocument): JsObject = JsObjectReader.read(obj)

}