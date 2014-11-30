package org.qbproject.mongo

import play.api.libs.json.JsObject

import scala.concurrent.Future

trait QBMongoCollectionInterface {

  type ID = String

  def count: Future[Int]

  def all(skip: Int = 0, limit: Int = 100): Future[List[JsObject]]

  def findOne(query: JsObject): Future[Option[JsObject]]

  def find(query: JsObject, skip: Int = 0, limit: Int = 100): Future[List[JsObject]]

  def findAndModify(query: JsObject, modifier: JsObject, upsert: Boolean = false): Future[Option[JsObject]]

  def findById(id: ID): Future[Option[JsObject]]

  def delete(id: ID): Future[Boolean]

  def create(obj: JsObject): Future[JsObject]

  def update(id: ID, update: JsObject): Future[JsObject]

  def update(query: JsObject, update: JsObject): Future[JsObject]

}

object MongoOp extends Enumeration {
  type MongoOp = Value
  val Null, Count, All, FindOne, Find, FindAndModify, FindById, Delete, Create, UpdateById, UpdateByQuery = Value
}