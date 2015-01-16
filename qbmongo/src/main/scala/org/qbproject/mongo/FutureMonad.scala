package org.qbproject.mongo

import scala.concurrent.Future
import scalaz.{Monad, Functor}

/**
 * Created by Edgar on 11.01.2015.
 */
object FutureMonad {

  import scala.concurrent.ExecutionContext.Implicits.global

  // provide functor instance for Future
  implicit def FutureFunctor: Functor[Future] = new Functor[Future] {
    override def map[A, B](fa: Future[A])(f: (A) => B): Future[B] = fa.map(f)
  }

  implicit def FutureMonad: Monad[Future] = new Monad[Future] {
    override def point[A](a: => A): Future[A] = Future.successful(a)

    override def bind[A, B](fa: Future[A])(f: (A) => Future[B]): Future[B] = fa.flatMap(f)
  }
}