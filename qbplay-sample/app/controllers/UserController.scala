package controllers

import org.qbproject.controllers._
import org.qbproject.mongo.{QBMongoDefaultCollection, QBMongoCollectionInterface}
import org.qbproject.mongo._
import org.qbproject.routing._
import org.qbproject.schema.QBSchema._

import model.UserSchema
import play.api.libs.json.Json
import play.api.mvc.Action
import play.modules.reactivemongo.MongoController

object UserController extends MongoController
  with QBCrudController with QBRouter {

  override def collection: QBAdaptedMongoCollection =
    QBMongoDefaultCollection("users", db, UserSchema.user)

  override def qbRoutes = UserController.crudRoutes

  override def createSchema = UserSchema.user - "id"

  def view = JsonHeaders {
    Action {
      Ok(Json.toJson(UserSchema.userView))
    }
  }

  def model = JsonHeaders {
    Action {
      Ok(Json.toJson(UserSchema.user))
    }
  }
  
}
