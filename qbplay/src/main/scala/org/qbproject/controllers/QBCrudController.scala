package org.qbproject.controllers

import play.api.mvc.Action
import play.api.libs.json._
import play.api.libs.concurrent.Execution.Implicits._
import org.qbproject.routing._
import org.qbproject.mongo.{QBAdaptedMongoCollection, QBMongoCollection}

trait QBCrudController extends QBAPIController { self =>

  def collection: QBAdaptedMongoCollection

  // TODO: avoid recalc
  def createSchema = collection.schema
  def updateSchema = collection.schema

  // Routes --
  def getAllRoute       =  GET   / ?                  to getAll
  def getByIdRoute      =  GET   / string             to getById
  def createRoute       =  POST  / ?                  to create
  def updateRoute       =  POST  / string             to update
  def deleteByPostRoute =  POST  / "delete" / string  to delete
  def deleteRoute       =  DELETE / string            to delete


  def crudRoutes: List[QBRoute] = List(
    getAllRoute,
    getByIdRoute,
    createRoute,
    deleteRoute,
    deleteByPostRoute,
    updateRoute)

  // --

  def getAll = JsonHeaders {
    Action.async {
      collection.all().map { result =>
        Ok(Json.toJson(result))
      }
    }
  }

  def getById(id: String) = JsonHeaders {
    Action.async {
      collection.findById(id).map {
        case Some(result) => Ok(Json.toJson(result))
        case _ => NotFound(":(")
      }
    }
  }

  def count = JsonHeaders {
    Action.async {
      collection.count.map { result =>
        Ok(Json.toJson(Json.obj("count" -> result)))
      }
    }
  }

  def beforeCreate(jsValue: JsValue): JsValue = jsValue
  def afterCreate(jsObject: JsObject): JsObject = jsObject

  def create = JsonHeaders {
    ValidatingAction(createSchema, beforeValidate = beforeCreate).async { request =>
      collection.create(request.validatedJson.asInstanceOf[JsObject]).map {
        result =>
          Ok(Json.toJson(afterCreate(result)))
      }
    }
  }

  def beforeUpdate(jsValue: JsValue): JsValue = jsValue
  def afterUpdate(jsObject: JsObject): JsObject = jsObject

  def update(id: String) = JsonHeaders {
    ValidatingAction(updateSchema, beforeValidate = beforeUpdate).async { request =>
      collection.update(id, request.validatedJson.asInstanceOf[JsObject]).map {
        result =>
          Ok(Json.toJson(afterUpdate(result)))
      }
    }
  }

  // TODO: current behaviour also returns true, if there is no document to delete
  //       verify, whether this behavior is wanted or not
  def delete(id: String) =  Action.async {
    collection.delete(id).map { result =>
      Ok(Json.obj("deleteSuccess" -> result))
    }
  }
}