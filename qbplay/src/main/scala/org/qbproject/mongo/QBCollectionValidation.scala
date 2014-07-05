package org.qbproject.mongo

import org.qbproject.schema._
import org.qbproject.schema.{QBValidationException, QBClass, QBPartialValidator, QBSchema}
import QBSchema._

import play.api.libs.json._
import play.api.libs.concurrent.Execution.Implicits._

import scala.concurrent.Future
import scala.util.Success


/** Automagically validates json in operations on a QBCollection */
trait QBCollectionValidation extends QBMongoCollection { self: QBMongoCollection =>

  /** Will be used to validate inputs and outputs */
  def schema: QBClass

  // import the mongo <-> json transformations
  val transform = new MongoTransformer(schema)
  import transform.{ read => readFromMongo, write => in }

  // generic output validation
  private def out(result: JsObject) = {
    readFromMongo(result) match {
      case JsSuccess(jsObj: JsObject, _) => jsObj
      case err: JsError                  => throw new QBValidationException(err)
    }
  }

  private def out(result: Option[JsObject]) = {
    result.map { readFromMongo(_) match {
      case JsSuccess(jsObj: JsObject, _) => jsObj
      case err: JsError                  => throw new QBValidationException(err)
    }}
  }

  private def out(result: List[JsObject]) = {
    result.flatMap { readFromMongo(_).asOpt }
  }
  
  // private def out[F[JsObject]]
  //     (result: Future[F[JsObject]])
  //     (implicit mapper: Monad[F]): Future[F[JsObject]] = {
  //   result.map { container: F[JsObject] =>
  //     mapper(container)({
  //       readFromMongo(_) match {
  //         case JsSuccess(jsObj: JsObject, _) => jsObj
  //         case err: JsError => throw new QBValidationException(err)
  //       }
  //     })
  //   }
  // }

  abstract override def getAll(skip: Int = 0, limit: Int = 100): Future[List[JsObject]] = {
    super.getAll(skip, limit).map(out)
  }

  abstract override def getById(id: ID): Future[Option[JsObject]] = {
    super.getById(id).map(out)
  }

  abstract override def find(query: JsObject, skip: Int = 0, limit: Int = 100): Future[List[JsObject]] = {
    super.find(query, skip, limit).map(out)
  }

  abstract override def findOne(query: JsObject): Future[Option[JsObject]] = {
    super.findOne(query).map(out)
  }

  
  /** Raw access to collection api, use #update for input validation. */
  abstract override def findAndModify(query: JsObject, modifier: JsObject, upsert: Boolean = false): Future[Option[JsObject]] = {
    super.findAndModify(query, modifier, upsert).map(out)
  }

  abstract override def update(id: ID, update: JsObject): Future[JsObject] = {
    (in(update, otherValidator = Some(QBPartialValidator)) match {
      case JsSuccess(js: JsObject, _) => super.update(id, js)
      case err: JsError => Future.failed(new QBValidationException(err))
    }).map(out)
  }

  abstract override def update(query: JsObject, update: JsObject): Future[JsObject] = {
    (in(update, otherValidator = Some(QBPartialValidator)) match {
      case JsSuccess(upd: JsObject, _) => super.update(query, upd)
      case err: JsError => Future.failed(new QBValidationException(err))
    }).map(out)
  }

  abstract override def create(obj: JsObject): Future[JsObject] = {
    (in(obj, otherSchema = Some(schema ? "id")) match {
      case JsSuccess(js: JsObject, _) => super.create(js)
      case err: JsError => Future.failed(new QBValidationException(err))
    }).map(out)
  }
}