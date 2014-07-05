package org.qbproject.routing

import play.core.Router
import scala.runtime.AbstractPartialFunction
import play.api.mvc.RequestHeader
import play.api.mvc.Handler
import play.api.mvc.Action
import scala.collection.mutable.ListBuffer

import org.qbproject.routing.internal.QBRouterUtil.QBRouteCollector;

/**
 * Main router interface.
 */
trait QBRouter extends QBBaseRouter {

  /**
   * List of QBRoutes
   */
  def qbRoutes: List[QBRoute]

}

/**
 * Companion object to create a router by adding a list of routes.
 */
object QBRouter {
  def apply(_routes: List[QBRoute]): QBRouter = new QBRouter {
    val qbRoutes: List[QBRoute] = _routes
  }
  def apply(routes: QBRoute*): QBRouter = apply(routes.toList)
}

/**
 * Base implementation of the qbrouter. It implements Play's routes interface in order to hook into Play.
 */
trait QBBaseRouter extends Router.Routes {

  /**
   * List of QBRoutes
   */
  def qbRoutes: List[QBRoute]

  /**
   * Checks whether a route exists for a given request header.
   * The check is done by using a regex in the default implementation.
   * @param requestHeader the request
   */
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

  /**
   * @inheritdoc
   */
  def prefix = path
  /**
   * @inheritdoc
   */
  def setPrefix(prefix: String) = path = prefix

  /**
   * @inheritdoc
   */
  def routes = new AbstractPartialFunction[RequestHeader, Handler] {
    override def applyOrElse[A <: RequestHeader, B >: Handler](requestHeader: A, default: A => B) = {
      resolveHandler(requestHeader).getOrElse(default(requestHeader))
    }
    def isDefinedAt(requestHeader: RequestHeader) = routeExists(requestHeader)
  }

  /**
   * @inheritdoc
   */
  lazy val documentation = qbRoutes.map(route => {
    val docs = route.documentation
    (docs._1, prefix + docs._2, docs._3)
  })

}

/**
 * Allows to aggregate routes without listing them individually. HasRouteCollector adds an implicit route collector
 * into the scope.
 */
trait HasRouteCollector {
  self: QBBaseRouter =>
  implicit val routeCollector = new QBRouteCollector()
  override def qbRoutes = routeCollector.routes.toList
}
