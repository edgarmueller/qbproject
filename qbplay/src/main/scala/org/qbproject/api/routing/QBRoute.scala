package org.qbproject.api.routing

import play.api.mvc.{ RequestHeader, Handler }

/**
 *
 *
 * TODO: Provide nicer DSL for QBRoute
 */
trait QBRoute {

  /**
   * Returns the path of this route.
   */
  def path: String

  def copy(path: String = path): QBRoute

  /**
   * Whether this route is applicable for the given request header.
   *
   * @param namespace
   *            the namespace this route is part of
   * @param requestHeader
   *            the request header
   * @return true, if the route is applicable, false otherwise
   */
  def matches(namespace: String, requestHeader: RequestHeader): Boolean

  /**
   * Returns the handler to be called if this route matches.
   *
   * @param namespace
   *            the namespace this route is part of
   * @param requestHeader
   *            the request header
   * @return the handler, if any
   */
  def getHandler(namespace: String, requestHeader: RequestHeader): Option[Handler]

  def documentation: (String, String, String) = ("", path, "")

}
