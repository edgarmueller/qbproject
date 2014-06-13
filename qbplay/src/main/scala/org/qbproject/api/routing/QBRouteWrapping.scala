package org.qbproject.api.routing

import play.api.mvc.Handler
import play.api.mvc.RequestHeader
import play.api.mvc.Action

object QBRouteWrapping {

  // TODO can we solve this in a nicer way?
  protected def actionHandlerCast(fn: Action[_] => Action[_]): Handler => Handler = fn.asInstanceOf[Handler => Handler]

  implicit class RoutesExtensionOps(routes: List[QBRoute]) {
    def wrapWith(wrapper: Action[_] => Action[_]): List[QBWrappedRoute] =
      routes.map(baseRoute => new QBWrappedRoute(baseRoute, actionHandlerCast(wrapper)))

    def wrapWith(wrappers: List[Action[_] => Action[_]]): List[QBWrappedRoute] =
      routes.map(baseRoute => new QBWrappedRoute(baseRoute, wrappers.map(actionHandlerCast): _*))
  }

  implicit class RouteExtensionOps(route: QBRoute) {
    def wrapWith(wrappers: List[Action[_] => Action[_]]): QBWrappedRoute =
      new QBWrappedRoute(route, wrappers.map(actionHandlerCast(_)): _*)

    def wrapWith(wrapper: Action[_] => Action[_]): QBWrappedRoute =
      new QBWrappedRoute(route, actionHandlerCast(wrapper))
  }
}

class QBWrappedRoute(wrappedRoute: QBRoute, wrappers: (Handler => Handler)*) extends QBRoute {

  def getHandler(namespace: String, requestHeader: RequestHeader): Option[Handler] = {
    wrappedRoute.getHandler(namespace, requestHeader) match {
      case Some(action: Action[_]) =>
        Some(wrappers.foldLeft(action.asInstanceOf[Handler])((act, nextWrapper) => nextWrapper(act)))
      case _ => None
    }
  }

  /** Delegates */
  def path: String = wrappedRoute.path
  def copy(path: String = path): QBRoute = new QBWrappedRoute(wrappedRoute.copy(path), wrappers: _*)
  def matches(namespace: String, requestHeader: RequestHeader): Boolean = wrappedRoute.matches(namespace, requestHeader)
  override def documentation = wrappedRoute.documentation

}
