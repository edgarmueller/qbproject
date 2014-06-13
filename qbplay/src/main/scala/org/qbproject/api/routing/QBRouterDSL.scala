package org.qbproject.api.routing

import scala.collection.mutable.ListBuffer
import play.api.mvc.Handler
import org.qbproject.api.routing.QBRouterUtil._

object QBRouterDSL {

  val GET = "GET"
  val POST = "POST"
  val PUT = "PUT"
  val DELETE = "DELETE"

  val ? = "?"

  def string = StringParam
  def int = IntParam

  implicit def stringToMethod(methodName: String): MethodMatcher = new MethodMatcher(methodName)
  implicit def stringToRouteBuilder(method: String)(implicit collector: RouteCollector = VoidCollector) = new Builder0(method, "", collector)
  implicit def methodToRouteBuilder(method: MethodMatcher)(implicit collector: RouteCollector = VoidCollector) = new Builder0(method, "", collector)

  /**
   * Prefixes all given path with the given prefix.
   */
  object namespace {
    def apply(prefix: String)(routes: => List[QBRoute]): List[QBRoute] = {
      routes.map(copyRoute(prefix, _))
    }
    def apply(prefix: String, route: => QBRoute): QBRoute = copyRoute(prefix, route)
    private def copyRoute(prefix: String, route: QBRoute): QBRoute = {
      route.copy(path = QBRouterUtil.joinPaths("/", prefix, route.path))
    }
  }

  /**
   * Builder for simple Routes
   */
  import QBRouterUtil.joinPaths
  class Builder0(method: MethodMatcher, var path: String, collector: RouteCollector = VoidCollector) {
    def /(static: String) = { path = joinPaths(path, static); this }
    def /[N](dynamic: PathParam[N]) = { new Builder1[N](method, joinPaths(path, dynamic.regexString), dynamic, collector) }
    def to(handler: => Handler): SimpleRoute0 = { collector.addAndReturn(SimpleRoute0(method, new PathMatcher(path), () => handler)) }
  }
  class Builder1[A](method: MethodMatcher, var path: String, param1: PathParam[A], collector: RouteCollector = VoidCollector) {
    def /(static: String) = { path = joinPaths(path, static); this }
    def /[N](dynamic: PathParam[N]) = { new Builder2[A, N](method, joinPaths(path, dynamic.regexString), param1, dynamic, collector) }
    def to(handler: A => Handler) = { collector.addAndReturn(SimpleRoute1(method, new PathMatcher(path), param1, handler)) }
  }
  class Builder2[A, B](method: MethodMatcher, var path: String, param1: PathParam[A], param2: PathParam[B], collector: RouteCollector = VoidCollector) {
    def /(static: String) = { path = joinPaths(path, static); this }
    def /[N](dynamic: PathParam[N]) = { new Builder3[A, B, N](method, joinPaths(path, dynamic.regexString), param1, param2, dynamic, collector) }
    def to(handler: (A, B) => Handler) = { collector.addAndReturn(SimpleRoute2(method, new PathMatcher(path), param1, param2, handler)) }
  }
  class Builder3[A, B, C](method: MethodMatcher, var path: String, param1: PathParam[A], param2: PathParam[B], param3: PathParam[C], collector: RouteCollector = VoidCollector) {
    def /(static: String) = { path = joinPaths(path, static); this }
    def /[N](dynamic: PathParam[N]) = ???
    def to(handler: (A, B, C) => Handler) = { collector.addAndReturn(SimpleRoute3(method, new PathMatcher(path), param1, param2, param3, handler)) }
  }
}