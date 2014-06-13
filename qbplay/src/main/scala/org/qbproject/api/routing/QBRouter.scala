package org.qbproject.api.routing

import play.core.Router
import scala.runtime.AbstractPartialFunction
import play.api.mvc.RequestHeader
import play.api.mvc.Handler
import play.api.mvc.Action
import scala.collection.mutable.ListBuffer

// TODO: Composite router
trait QBRouter extends QBBaseRouter

object QBRouter {
  def apply(_routes: List[QBRoute]): QBRouter = new QBRouter {
    val qbRoutes: List[QBRoute] = _routes
  }
  def apply(routes: QBRoute*): QBRouter = apply(routes.toList)
}

trait QBBaseRouter extends Router.Routes {

  def qbRoutes: List[QBRoute]

  def routeExists(requestHeader: RequestHeader) = qbRoutes.exists(_.matches(prefix, requestHeader))

  def resolveHandler(requestHeader: RequestHeader): Option[Handler] = {
    qbRoutes
      .find(_.matches(prefix, requestHeader))
      .flatMap(_.getHandler(prefix, requestHeader))
  }

  /**
   * Below: Methods to satisfy Play's Router.Routes
   */
  private var path: String = ""
  def prefix = path
  def setPrefix(prefix: String) = path = prefix

  def routes = new AbstractPartialFunction[RequestHeader, Handler] {
    override def applyOrElse[A <: RequestHeader, B >: Handler](requestHeader: A, default: A => B) = {
      resolveHandler(requestHeader).getOrElse(default(requestHeader))
    }
    def isDefinedAt(requestHeader: RequestHeader) = routeExists(requestHeader)
  }

  lazy val documentation = qbRoutes.map(route => {
	  val docs = route.documentation
	  (docs._1, prefix+docs._2, docs._3)
  })

}
