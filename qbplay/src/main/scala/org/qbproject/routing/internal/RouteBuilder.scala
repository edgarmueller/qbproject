package org.qbproject.routing.internal

import org.qbproject.routing.internal.QBRouterUtil._
import play.api.mvc.Handler

/**
 * Builder for simple Routes
 */

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

class Builder4[A, B, C, D](method: MethodMatcher, var path: String, param1: PathParam[A], param2: PathParam[B], param3: PathParam[C], param4: PathParam[D], collector: RouteCollector = VoidCollector) {
  def /(static: String) = { path = joinPaths(path, static); this }
  def /[N](dynamic: PathParam[N]) = { new Builder4[A, B, C, N](method, joinPaths(path, dynamic.regexString), param1, param2, param3, dynamic, collector) }
  def to(handler: (A, B, C, D) => Handler) = { collector.addAndReturn(SimpleRoute4(method, new PathMatcher(path), param1, param2, param3, param4, handler)) }
}