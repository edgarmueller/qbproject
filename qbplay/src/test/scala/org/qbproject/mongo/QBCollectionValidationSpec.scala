package org.qbproject.mongo

import play.api.libs.json._
import reactivemongo.bson.BSONObjectID

import org.specs2.mutable.Specification
import org.joda.time.DateTime
import org.qbproject.schema.QBSchema._
import org.qbproject.schema.QBClass
import scala.concurrent.Future

class QBCollectionValidationSpec extends Specification {

import QBCollectionValidationSpec._

  "QBCollectionValidation on a mocked QBMongoCollection" should {
    // note that the input is required to be the sampleJson,
    // since the mockup class with check if it gets the transformed 
    // version of this sample where required

    "validate getAll" in {
      mock.getAll() must containAllOf(List(sampleJson, sampleJson)).await
    }

    "validate getById" in {
      mock.getById("not used") must beSome(sampleJson).await
    }

    "validate find" in {
      mock.find(sampleJson) must containAllOf(List(sampleJson)).await
    }

    "validate findOne" in {
      mock.findOne(sampleJson) must beSome(sampleJson).await
    }

    "validate findAndModify" in {
      mock.findAndModify(sampleJson, sampleJson) must beSome(sampleJson).await
    }

    "validate update with id" in {
      mock.update(id, sampleJson) must beEqualTo(sampleJson).await
    }

    "validate update with id and partial input" in {
      mock.update(id, sampleWithDateOnly) must beEqualTo(sampleJson).await
    }

    "validate update with query" in {
      mock.update(sampleJson, sampleJson) must beEqualTo(sampleJson).await
    }

    "validate create" in {
      mock.create(sampleJson) must beEqualTo(sampleJson).await
    }

    "validate delete" in {
      mock.create(sampleJson) must beEqualTo(sampleJson).await
      mock.delete(id) must beEqualTo(true).await
    }

    "validate create on json with no id" in {
      mock.create(sampleCreateJson) must beEqualTo(sampleJson).await
    }
  }
}

object QBCollectionValidationSpec {
  // sample values
  val id = BSONObjectID.generate.stringify
  val date = new DateTime().toString()
  val time = System.currentTimeMillis() / 1000L

  val sampleSchema = qbClass(
    "id" -> objectId,
    "d" -> qbDateTime,
    "e" -> qbPosixTime,
    "i" -> qbClass("x" -> objectId))

  val sampleJson = Json.obj(
    "id" -> id,
    "d" -> date,
    "e" -> time,
    "i" -> Json.obj("x" -> id))

  val sampleWithDateOnly = Json.obj(
    "d" -> date)

  val sampleCreateJson = Json.obj(
    "d" -> date,
    "e" -> time,
    "i" -> Json.obj("x" -> id))

  val mongoJson = Json.obj(
    "_id" -> Json.obj("$oid" -> id),
    "d" -> Json.obj("$date" -> date),
    "e" -> Json.obj("$date" -> time),
    "i" -> Json.obj("x" -> Json.obj("$oid" -> id)))

  val mongoWithDateOnly = Json.obj(
    "d" -> Json.obj("$date" -> date))

  val mongoTransformer = new MongoTransformer(sampleSchema)
  val result = mongoTransformer.toMongoJson(sampleJson)

  val mock = new QBMockupCollection with QBCollectionValidation {
    val schema = sampleSchema // also test lazy init of the anonymous class here
  }

  // sample collection
  class QBMockupCollection extends QBMongoCollection(null)(null) {
    override def getCount = Future.successful(1)

    override def getAll(skip: Int = 0, limit: Int = 100): Future[List[JsObject]] = {
      // insert a non-validating json obj, this should be filtered out later
      Future.successful(List(mongoJson, Json.obj("notThere" -> 1337), mongoJson, Json.obj("notThere" -> 1337)))
    }

    override def getById(id: ID): Future[Option[JsObject]] = {
      Future.successful(Some(mongoJson))
    }

    override def find(query: JsObject, skip: Int = 0, limit: Int = 100): Future[List[JsObject]] = {
      if (query == sampleJson) Future.successful(List(mongoJson))
      else Future.failed(null)
    }

    override def findOne(query: JsObject): Future[Option[JsObject]] = {
      if (query == sampleJson) Future.successful(Option(mongoJson))
      else Future.failed(null)
    }

    override def findAndModify(query: JsObject, modifier: JsObject, upsert: Boolean = false): Future[Option[JsObject]] = {
      // no validation on the input done, only transformation on the output
      Future.successful(Some(mongoJson))
    }

    override def update(id: ID, update: JsObject): Future[JsObject] = {
      if ((update == mongoJson) ||
          (update == mongoWithDateOnly)) {
        Future.successful(mongoJson)
      } else {
        Future.failed(null)
      }
    }

    override def update(query: JsObject, update: JsObject): Future[JsObject] = {
      if ((query == sampleJson) &&
          (update == mongoJson)) {
        Future.successful(mongoJson)
      } else {
        Future.failed(null)
      }
    }

    override def create(obj: JsObject): Future[JsObject] = {
      Future.successful(mongoJson)
    }

    // TODO: mock should act upon underlying collection
    override def delete(objId: ID): Future[Boolean] = {
      if (objId == id) {
        Future.successful(true)
      } else {
        Future.successful(false)
      }
    }
  }
}
