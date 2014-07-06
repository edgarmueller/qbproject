package org.qbproject.routing.internal

import play.api.mvc.RequestHeader
import scala.collection.mutable.ListBuffer
import org.qbproject.routing._

object QBRouterUtil {

  /**
   * Drops the namespace prefix of the request header's path, if it matches.
   *
   * @param prefix
   *            the namespace prefix to be dropped
   * @param requestHeader
   *            the request header whose namespace prefix should be dropped
   * @return the path of the request header without the matched prefix, if the prefix matched, None otherwise
   */
  def cutPath(prefix: String, requestHeader: RequestHeader): Option[String] = {
    if (requestHeader.path.startsWith(prefix)) {
      Some(requestHeader.path.drop(prefix.length))
    } else {
      None
    }
  }

  /**
   * Joins parts of a URL path to one path.
   *
   * @param pathParts
   */
  def joinPaths(pathParts: String*): String = joinPaths(false)(pathParts: _*)

  /**
   * Joins parts of a URL path to one path.
   *
   * @param ensureStartingSlash ensures starting /
   * @param pathParts
   */
  def joinPaths(ensureStartingSlash: Boolean = false)(pathParts: String*): String = {
    pathParts.map(_.trim).filterNot(_ == "")
      .foldLeft("")((url, next) => {
        val priorHasSlash = url.endsWith("/")
        val nextHasSlash = next.startsWith("/") || (url == "" && !ensureStartingSlash)

        (priorHasSlash, nextHasSlash) match {
          case (true, false) => url + next
          case (false, true) => url + next
          case (true, true) => url + next.substring(1)
          case (false, false) => url + "/" + next
        }
      })
  }

  trait RouteCollector {
    def addAndReturn[R <: QBRoute](route: R): R
  }
  case object VoidCollector extends RouteCollector {
    def addAndReturn[R <: QBRoute](route: R): R = route
  }
  case class QBRouteCollector(routes: ListBuffer[QBRoute] = ListBuffer()) extends RouteCollector {
    def addAndReturn[R <: QBRoute](route: R): R = {
      routes.append(route)
      route
    }
    def toRouter: QBRouter = QBRouter(routes: _*)
  }
}