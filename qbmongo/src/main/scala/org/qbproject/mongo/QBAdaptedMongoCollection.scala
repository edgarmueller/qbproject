package org.qbproject.mongo

import com.typesafe.scalalogging.LazyLogging
import org.qbproject.mongo.MongoOp.MongoOp
import org.qbproject.schema.{QBClass, QBValidationException}
import play.api.libs.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}
import scalaz.OptionT

class QBAdaptedMongoCollection(collection: QBMongoCollectionInterface, qbClass: QBClass, adapter: MongoConversion)
  extends QBMongoCollectionInterface with LazyLogging {

  import org.qbproject.mongo.FutureMonad._

  def schema = qbClass

  override def count: Future[Int] = {
    implicit val op = MongoOp.Count
    collection.count
  }

  override def findById(id: ID): Future[Option[JsObject]] = {
    implicit val op = MongoOp.FindById
    val findFuture: Future[Option[JsObject]] = collection.findById(id)
    val f = OptionT[Future, JsObject](findFuture).flatMapF[JsObject](outBound).run
    f.onComplete {
      case Success(success) => logger.debug(s"[findById - Success (id: $id)], found $success")
      case Failure(failure) => logger.debug(s"[findById - Failure (id: $id)] $failure")
    }
    f
  }

  override def update(id: ID, update: JsObject): Future[JsObject] = {
    implicit val op = MongoOp.UpdateById
    // TODO: it's so hard to debug this stuff, if fields are filtered our for validation constraint reasons..
    val f = inBound(update).flatMap(e => collection.update(id, e)).flatMap(outBound)
    f.onComplete {
      case Success(success) => logger.debug(s"[update - Success (id: $id)], found $success")
      case Failure(failure) => logger.debug(s"[update - Failure (id: $id)] $failure")
    }
    f
  }

  override def update(query: JsObject, update: JsObject): Future[JsObject] = {
    implicit val op = MongoOp.UpdateByQuery
    inBound(update).flatMap(collection.update(query, _).flatMap(outBound))
  }

  override def findOne(query: JsObject): Future[Option[JsObject]] = {
    implicit val op = MongoOp.FindOne
    OptionT(collection.findOne(query)).flatMapF(outBound).run
  }

  override def findAndModify(query: JsObject, modifier: JsObject, upsert: Boolean): Future[Option[JsObject]] = {
    implicit val op = MongoOp.FindAndModify
    OptionT(collection.findAndModify(query, modifier, upsert)).flatMapF(outBound).run
  }

  override def all(skip: Int, limit: Int): Future[List[JsObject]] = {
    implicit val op = MongoOp.All
    collection.all(skip, limit).flatMap(outBoundMany)
  }

  override def find(query: JsObject, skip: Int, limit: Int): Future[List[JsObject]] = {
    implicit val op = MongoOp.Find
    val f = for {
      res <- collection.find(query, skip)
      o <- outBoundMany(res)
    } yield o
    f.onComplete {
      case Success(success) => logger.debug(s"[find - Success (query: $query)], found $success")
      case Failure(failure) => logger.debug(s"[find - Failure (query: $query)] $failure")
    }
    f
  }

  override def delete(id: ID): Future[Boolean] = {
    implicit val op = MongoOp.Delete
    collection.delete(id)
  }

  override def create(obj: JsObject): Future[JsObject] = {
    implicit val op = MongoOp.Create
    val x = inBound(obj)
    x.flatMap(collection.create).flatMap(outBound)
  }

  private def inBound(obj: JsObject)(implicit op: MongoOp): Future[JsObject] = {
    adapter.toMongoJson(op)(obj) match {
      case err: JsError => Future.failed(new QBValidationException(err))
      case JsSuccess(upd: JsObject, _) => Future.successful(upd)
    }
  }

  private def outBoundMany(objs: List[JsObject])(implicit op: MongoOp): Future[List[JsObject]] = {
    Future.successful(objs.map(adapter.fromMongoJson(op)(_).asOpt).flatten)
  }

  private def outBound(obj: JsObject)(implicit op: MongoOp): Future[JsObject] = {
    adapter.fromMongoJson(op)(obj) match {
      case err: JsError => Future.failed(new QBValidationException(err))
      case JsSuccess(upd: JsObject, _) => Future.successful(upd)
    }
  }

}
