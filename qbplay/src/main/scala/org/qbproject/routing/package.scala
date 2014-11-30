package org.qbproject

import play.api.mvc.Handler
import org.qbproject.routing.internal.QBRouterUtil._
import org.qbproject.routing.internal._

package object routing extends QBRouteWrapping {

  val GET = "GET"
  val POST = "POST"
  val PUT = "PUT"
  val DELETE = "DELETE"
  val OPTIONS = "OPTIONS"
  val HEAD = "HEAD"

  val ? = "?"

  def string = StringParam
  def int = IntParam
  def long = LongParam
  def double = DoubleParam

  /**
   * Router DSL. Start for the route builder GET / "something"
   */
  implicit def stringToMethod(methodName: String): MethodMatcher = new MethodMatcher(methodName)
  implicit def stringToRouteBuilder(method: String)(implicit collector: RouteCollector = VoidCollector) = new Builder0(stringToMethod(method), "/", collector)
  implicit def methodToRouteBuilder(method: MethodMatcher)(implicit collector: RouteCollector = VoidCollector) = new Builder0(method, "/", collector)

  /**
   * Prefixes all given path with the given prefix.
   */
  object namespace {
    def apply(prefix: String)(routes: => List[QBRoute]): List[QBRoute] = {
      routes.map(copyRoute(prefix, _))
    }
    def apply(prefix: String, route: => QBRoute): QBRoute = copyRoute(prefix, route)
    private def copyRoute(prefix: String, route: QBRoute): QBRoute = {
      route.copy(path = QBRouterUtil.joinPaths(true)(prefix, route.path))
    }
  }

}