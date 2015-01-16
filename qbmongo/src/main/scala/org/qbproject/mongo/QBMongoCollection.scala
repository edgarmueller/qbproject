package org.qbproject.mongo

import play.api.libs.json._
import play.modules.reactivemongo.json.collection.JSONCollection
import reactivemongo.api.{QueryOpts, DB}
import reactivemongo.bson.{BSONDocument, BSONObjectID}
import reactivemongo.core.commands.{FindAndModify, Update, Count}
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

class QBMongoCollection(collectionName: String)(db: DB) extends QBMongoCollectionInterface {

  private lazy val collection: JSONCollection = db.collection[JSONCollection](collectionName)
  
  /** Perform a raw command on the underlying mongocollection */
  def rawCollection = collection

  def count = db.command(Count(collectionName))

  def all(skip: Int = 0, limit: Int = 100): Future[List[JsObject]] = find(Json.obj(), skip, limit)

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
      Update(modifier, fetchNewObject = true),
      upsert
    )).map(_.map(bsonToJson))
  }

  def findById(id: ID): Future[Option[JsObject]] = {
    collection.find(Json.obj("_id" -> Json.obj("$oid" -> id))).cursor[JsObject].headOption
  }

  def delete(id: ID): Future[Boolean]  = {
    val selector = Json.obj("_id" -> Json.obj("$oid" -> id))
    collection.remove(selector).map { lastError =>
      true
    } recover {
      // TODO: log exception or return in it json object
      case exception => false
    }
  }

  def create(obj: JsObject): Future[JsObject] = {
    val toCreate = obj \ "_id" match {
      case no: JsUndefined => obj ++ Json.obj("_id" -> Json.obj(
        "$oid" -> BSONObjectID.generate.stringify
      ))
      case _ => obj
    }

    collection.insert(toCreate).map(lastError =>
      if (lastError.ok) toCreate 
      else throw new RuntimeException("oh noes."))
  }

  def update(id: ID, updatedObject: JsObject): Future[JsObject] = {
    // remove _id field if present
    val update = updatedObject.-("_id")
    val query = Json.obj("_id" -> Json.obj("$oid" -> id))
    val updateJson = update \ "$set" match {
      case un: JsUndefined => Json.obj("$set" -> update)
      case _ => update
    }
    this.findAndModify(query, updateJson).map {
      case Some(js) => js
      case None     => throw new RuntimeException("No Result found.")
    }
  }

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

  import play.modules.reactivemongo.json.ImplicitBSONHandlers._

  implicit def jsonToBson(obj: JsObject): BSONDocument = JsObjectWriter.write(obj)

  implicit def bsonToJson(obj: BSONDocument): JsObject = JsObjectReader.read(obj)

}