package org.qbproject.mongo

import org.qbproject.mongo.MongoOp.MongoOp
import org.qbproject.schema.{QBValidationException, QBClass}
import play.api.libs.json._

import scala.concurrent.Future
import scalaz.{OptionT, Monad, Functor}
import scala.concurrent.ExecutionContext.Implicits.global

class QBAdaptedMongoCollection(collection: QBMongoCollectionInterface, qbClass: QBClass, adapter: MongoConversion) extends QBMongoCollectionInterface {

  // provide functor instance for Future
  implicit def FutureFunctor: Functor[Future] = new Functor[Future] {
    override def map[A, B](fa: Future[A])(f: (A) => B): Future[B] = fa.map(f)
  }

  implicit def FutureMonad: Monad[Future] = new Monad[Future] {
    override def point[A](a: => A): Future[A] = Future.successful(a)
    override def bind[A, B](fa: Future[A])(f: (A) => Future[B]): Future[B] = fa.flatMap(f)
  }

  def schema = qbClass

  override def count: Future[Int] = {
    implicit val op = MongoOp.Count
    collection.count
  }

  override def findById(id: ID): Future[Option[JsObject]] = {
    implicit val op = MongoOp.FindById
    OptionT[Future, JsObject](collection.findById(id)).flatMapF(outBound).run
  }

  override def update(id: ID, update: JsObject): Future[JsObject] = {
    implicit val op = MongoOp.UpdateById
    inBound(update).flatMap(collection.update(id, _).flatMap(outBound))
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
    collection.find(query, skip).flatMap(outBoundMany)
  }

  override def delete(id: ID): Future[Boolean] = {
    implicit val op = MongoOp.Delete
    collection.delete(id)
  }

  override def create(obj: JsObject): Future[JsObject] = {
    implicit val op = MongoOp.Create
    inBound(obj).flatMap(collection.create(_).flatMap(outBound))
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
