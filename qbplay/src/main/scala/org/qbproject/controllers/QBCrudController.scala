package org.qbproject.controllers

import play.api.mvc.Action
import play.api.libs.json._
import play.api.libs.concurrent.Execution.Implicits._
import org.qbproject.routing._
import org.qbproject.mongo.{ QBCollectionValidation, QBMongoCollection }

trait QBCrudController extends QBAPIController { self =>

  def collection: QBMongoCollection with QBCollectionValidation

  // TODO: avoid recalc
  def createSchema = collection.schema
  def updateSchema = collection.schema

  // Routes --
  def getAllRoute =  GET    / ?             to getAll
  def getByIdRoute = GET    /  string       to getById
  def createRoute =  POST   / ?             to create
  def updateRoute =  POST   /  string       to update 

  def crudRoutes: List[QBRoute] = List(
    getAllRoute,
    getByIdRoute,
    createRoute,
    updateRoute)

  // --

  def getAll = JsonHeaders {
    Action.async {
      collection.getAll().map { result =>
        Ok(Json.toJson(result))
      }
    }
  }

  def getById(id: String) = JsonHeaders {
    Action.async {
      collection.getById(id).map {
        case Some(result) => Ok(Json.toJson(result))
        case _ => NotFound(":(")
      }
    }
  }

  def count = JsonHeaders {
    Action.async {
      collection.getCount.map { result =>
        Ok(Json.toJson(Json.obj("count" -> result)))
      }
    }
  }

  def beforeCreate(jsValue: JsValue): JsValue = jsValue
  def afterCreate(jsObject: JsObject): JsObject = jsObject

  def create = JsonHeaders {
    ValidatingAction(createSchema, beforeCreate).async { request =>
      collection.create(request.validatedJson.asInstanceOf[JsObject]).map {
        result =>
          Ok(Json.toJson(afterCreate(result)))
      }
    }
  }

  def beforeUpdate(jsValue: JsValue): JsValue = jsValue
  def afterUpdate(jsObject: JsObject): JsObject = jsObject

  def update(id: String) = JsonHeaders {
    ValidatingAction(updateSchema, beforeUpdate).async { request =>
      collection.update(id, request.validatedJson.asInstanceOf[JsObject]).map {
        result =>
          Ok(Json.toJson(afterUpdate(result)))
      }
    }
  }

}