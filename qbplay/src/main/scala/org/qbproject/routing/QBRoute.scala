package org.qbproject.routing

import play.api.mvc.{ RequestHeader, Handler }

/**
 * Super interface for all routes.
 */
trait QBRoute {

  /**
   * Returns the path of this route.
   */
  def path: String

  /**
   * Allows to copy the route and modify the path.
   * @param path new path
   */
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

  /**
   * Returns the documentation for Play's routing page and other uses.
   * Format is `GET   /path    comment/controller
   */
  def documentation: (String, String, String) = ("", path, "")

}
