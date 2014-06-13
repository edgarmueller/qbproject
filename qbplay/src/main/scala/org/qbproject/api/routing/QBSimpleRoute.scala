package org.qbproject.api.routing

import play.api.mvc.{ RequestHeader, Handler }
import scala.util.matching.Regex
import org.qbproject.api.routing.QBRouterDSL._

trait QBSimpleRoute extends QBRoute {

  def matches(prefix: String, requestHeader: RequestHeader): Boolean = {
    QBRouterUtil.cutPath(prefix, requestHeader).exists { requestPath =>
      // println(s"[SimpelRoute] $pathMatcher matches $requestPath ... ${pathMatcher.matches(requestPath)}")
      pathMatcher.matches(requestPath) &&
        methodMatcher.matches(requestHeader.method)
    }
  }

  def path = pathMatcher.toString
  def pathMatcher: PathMatcher
  def methodMatcher: MethodMatcher
  
  override def documentation = (methodMatcher.toString, pathMatcher.toString, "# dynamic QB Router #")
}

class MethodMatcher(name: String) {
  def matches(otherMethod: String) = name == otherMethod
  override val toString = name
}

class PathMatcher(path: String) {
  private val regex = path.r
  def matches(pathToMatch: String): Boolean = regex.pattern.matcher(pathToMatch).matches
  def unapplySeq(target: Any): Option[List[String]] = regex.unapplySeq(target)
  def withNewPath(path: String): PathMatcher = new PathMatcher(path)
  override val toString = regex.toString
}

trait PathParam[A] {
  def regexString: String
  def apply(str: String): A
}

// TODO more path params
case object StringParam extends PathParam[String] {
  def regexString: String = "(.*?)"
  def apply(str: String): String = str
}

case object IntParam extends PathParam[Int] {
  def regexString: String = "([0-9]+)"
  def apply(str: String): Int = str.toInt
}

case class SimpleRoute0(methodMatcher: MethodMatcher, pathMatcher: PathMatcher, handler: () => Handler) extends QBSimpleRoute {
  def getHandler(namespace: String, requestHeader: RequestHeader): Option[Handler] = Some(handler())
  def copy(path: String = path): SimpleRoute0 = {
    new SimpleRoute0(methodMatcher, pathMatcher.withNewPath(path), handler)
  }
}

case class SimpleRoute1[A](methodMatcher: MethodMatcher, pathMatcher: PathMatcher, pm1: PathParam[A], handler: A => Handler) extends QBSimpleRoute {
  def getHandler(namespace: String, requestHeader: RequestHeader): Option[Handler] =
    QBRouterUtil.cutPath(namespace, requestHeader).flatMap {
      case pathMatcher(p1) => Some(handler(pm1(p1)))
      case _ => None
    }

  def copy(path: String = path): SimpleRoute1[A] = {
    new SimpleRoute1(methodMatcher, pathMatcher.withNewPath(path), pm1, handler)
  }
}

case class SimpleRoute2[A, B](methodMatcher: MethodMatcher, pathMatcher: PathMatcher, pm1: PathParam[A], pm2: PathParam[B], handler: (A, B) => Handler) extends QBSimpleRoute {
  def getHandler(namespace: String, requestHeader: RequestHeader): Option[Handler] =
    QBRouterUtil.cutPath(namespace, requestHeader).flatMap {
      case pathMatcher(p1, p2) => Some(handler(pm1(p1), pm2(p2)))
      case _ => None
    }

  def copy(path: String = path): SimpleRoute2[A, B] = {
    new SimpleRoute2(methodMatcher, pathMatcher.withNewPath(path), pm1, pm2, handler)
  }
}

case class SimpleRoute3[A, B, C](methodMatcher: MethodMatcher, pathMatcher: PathMatcher, pm1: PathParam[A], pm2: PathParam[B], pm3: PathParam[C], handler: (A, B, C) => Handler) extends QBSimpleRoute {
  def getHandler(namespace: String, requestHeader: RequestHeader): Option[Handler] =
    QBRouterUtil.cutPath(namespace, requestHeader).flatMap {
      case pathMatcher(p1, p2, p3) => Some(handler(pm1(p1), pm2(p2), pm3(p3)))
      case _ => None
    }

  def copy(path: String = path): SimpleRoute3[A, B, C] = {
    new SimpleRoute3(methodMatcher, pathMatcher.withNewPath(path), pm1, pm2, pm3, handler)
  }
}