package org.qbproject.sample.akka

import akka.actor.Actor
import org.qbproject.mongo.{QBCollectionValidation, QBMongoCollection, _}
import org.qbproject.schema.QBSchema._
import org.qbproject.schema.QBValidator
import play.api.libs.json.{JsError, JsObject, Json}
import reactivemongo.api.MongoDriver
import spray.http.{ContentTypes, HttpEntity, HttpResponse, StatusCodes}
import spray.httpx.PlayJsonSupport._
import spray.routing.HttpService

import scala.concurrent.Future

class SampleServiceActor extends Actor with SampleService {
  implicit def actorRefFactory = context
  def receive = runRoute(myRoute)
}

trait SampleService extends HttpService {
  import scala.concurrent.ExecutionContext.Implicits.global


  val personSchema = qbClass(
    "id" -> objectId,
    "name" -> qbString(minLength(3)),
    "age" -> qbNumber,
    "email" -> optional(qbEmail)
  )

  val driver = new MongoDriver
  val connection = driver.connection(List("localhost"))

  // Gets a reference to the database "plugin"
  val db = connection("persons")

  def collection: QBCollectionValidation =
    new QBMongoCollection("persons-coll")(db) with QBCollectionValidation {
      override def schema = personSchema

    }

  lazy val myRoute =
    path("person") {
      put {
        putRoute
      } ~
      get {
        getRoute
      } ~
      delete {
        deleteRoute
      }
    }

  protected lazy val putRoute =
    entity(as[JsObject]) { jsObject ⇒
        complete {
          QBValidator.validate[JsObject](personSchema ? "id")(jsObject) match {
            case err: JsError => Future.successful(JsError.toFlatJson(err))
            case succ => collection.create(succ.get).map(obj =>
              HttpResponse(
                StatusCodes.OK,
                HttpEntity(ContentTypes.`application/json`, Json.stringify(obj))
              ))
          }
        }
    }

  protected lazy val getRoute = parameters('id.as[String]) { id ⇒
        complete {
          collection.getById(id)
        }
      } ~
        complete {
          collection.getAll()
      }

  protected lazy val deleteRoute = parameters('id.as[String]) { id =>
      complete {
        collection.delete(id)
      }
    }
}
