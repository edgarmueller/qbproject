package controllers

import play.modules.reactivemongo.MongoController
import play.api.mvc.Controller

import play.api.mvc.Action
import play.modules.reactivemongo.json.collection.JSONCollection
import play.api.libs.json._
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import scala.concurrent.Future

import org.qbproject.schema.QBSchema
import QBSchema._
import org.qbproject.api.mongo.{MongoSchemaExtensions, QBMongoCollection}
import MongoSchemaExtensions._

import scala.util.Failure
import reactivemongo.bson.{ BSONDocument, BSONObjectID }
import play.modules.reactivemongo.json.BSONFormats._

import play.api.mvc.SimpleResult
import org.qbproject.api.controllers.QBAPIController
import org.qbproject.api.mongo.QBMongoCollection

trait BugController extends Controller with MongoController with QBAPIController {

  lazy val bugsCol = new QBMongoCollection("bugs")(db)

  def getBugs = Action.async {
    bugsCol.getAll().map {
      result =>
        Ok(Json.toJson(result))
    }
  }

  def createBug = ValidatingAction(SampleSchema.bug).async {
    request =>
      bugsCol.collection.insert(request.validatedJson).map {
        error =>
          if (error.ok) Ok("Bug created")
          else BadRequest(error.message)
      }
  }

  def getSchema = Action {
    Ok(Json.toJson(SampleSchema.bug))
  }

  def getCommentSchema = Action {
    val schema = qbClass(
      "id" -> objectId,
      "companyId" -> objectId, // (endpoint für autocomplete)
      "bookingId" -> optional(objectId), // (endpoint für autocomplete)
      "ratingStatus" -> qbEnum("pending", "approved", "rejected"),
      "rating" -> qbInteger(range(1, 5)),
      "ratingOf" -> qbInteger(range(5, 5)), // readonly?
      "comment" -> qbString(maxLength(2000)),
      "firstName" -> qbString(length(1,50)),
      "lastName" -> qbString(length(1,50)),
      "ratingDate" -> qbDateTime
    )
    Ok(Json.toJson(schema))
  }

  def getBugById(bugId: String) = Action.async {
    request =>
      val query = BSONDocument(
        "_id" -> BSONObjectID(bugId))
      bugsCol.collection
        .find(query)
        .cursor[JsValue]
        .collect[List]()
        .map {
          result =>
            Ok(Json.toJson(result))
        }
  }
}

object BugController extends BugController