package org.qbproject.api.routing

import org.specs2.mutable._
import org.qbproject.api.routing.QBRouterDSL.{ GET => qbGET, POST => qbPOST, _ }
import org.qbproject.api.routing.QBRouteWrapping._
import org.qbproject.api.routing.QBRouterUtil.QBRouteCollector
import play.core.Router
import play.api.mvc._
import play.api.test.WithApplication
import play.api.test.FakeRequest
import play.api.test.PlaySpecification
import play.api.test.FakeApplication

class QBBaseRouterSpec extends PlaySpecification {

  object TestController extends Controller {

    // Actions
    def root = Action { Ok("root") }
    def foo = Action { Ok("foo") }
    def bar = Action { Ok("bar") }

    // Routes (qb Prefixes due to import conflict)
    val rootRoute = GET / ? to root
    val fooRoute = GET / "foo" / ? to foo
    val barRoute = POST / "bar" to bar
    val foobar = GET / "foo" / "bar" to foo
    val foobar2 = GET / "bar/foo" to bar

    val allRoutes = List(rootRoute, fooRoute, barRoute, foobar, foobar2)

    val FakeAppWithRouter = new FakeApplication {
      override lazy val routes: Option[Router.Routes] =
        Some(QBRouter(allRoutes))
    }
  }

  "QBBaseRouter" should {

    "resolve a simple root GET route" in new WithApplication(TestController.FakeAppWithRouter) {
      val result = route(FakeRequest(GET, ""))
      result.map(contentAsString) must beSome("root")
    }

    "resolve a simple GET route" in new WithApplication(TestController.FakeAppWithRouter) {
      val result = route(FakeRequest(GET, "/foo"))
      result.map(contentAsString) must beSome("foo")
    }

    "resolve a simple GET route with trailing slash" in new WithApplication(TestController.FakeAppWithRouter) {
      val result = route(FakeRequest(GET, "/foo/"))
      result.map(contentAsString) must beSome("foo")
    }

    "not resolve a simple POST route on anothers route path" in new WithApplication(TestController.FakeAppWithRouter) {
      val result = route(FakeRequest(POST, "/foo"))
      result.map(contentAsString) must beNone
    }

    "resolve a simple POST route" in new WithApplication(TestController.FakeAppWithRouter) {
      val result = route(FakeRequest(POST, "/bar"))
      result.map(contentAsString) must beSome("bar")
    }

    "resolve a route with two static parts" in new WithApplication(TestController.FakeAppWithRouter) {
      val result = route(FakeRequest(GET, "/foo/bar"))
      result.map(contentAsString) must beSome("foo")
    }

    "resolve a route with two static defined as one" in new WithApplication(TestController.FakeAppWithRouter) {
      val result = route(FakeRequest(GET, "/bar/foo"))
      result.map(contentAsString) must beSome("bar")
    }
  }

  object DynamicTestController extends Controller {

    // Actions
    def echo(str: String) = Action { Ok(str) }
    def number(int: Int) = Action { Ok("Number: " + int) }
    def echoBoth(int: Int, str: String) = Action { Ok(int + " " + str) }
    def twoString(str1: String, str2: String) = Action { Ok(str1 + " " + str2) }

    val sayHello = GET / "sayHello" / string to echo
    val numberRoute = GET / "echonr" / int to number
    val bothRoute = GET / "both" / int / string to echoBoth
    val twoStringRoute = GET / "one" / string / "two" / string to twoString

    val allRoutes = List(numberRoute, sayHello, bothRoute, twoStringRoute)

    val FakeAppWithRouter = new FakeApplication {
      override lazy val routes: Option[Router.Routes] =
        Some(QBRouter(allRoutes))
    }
  }

  "QBBaseRouter" should {

    "resolve route with dynamic string part" in new WithApplication(DynamicTestController.FakeAppWithRouter) {
      val result = route(FakeRequest(GET, "/sayHello/World"))
      result.map(contentAsString) must beSome("World")
    }

    "resolve route with dynamic int part" in new WithApplication(DynamicTestController.FakeAppWithRouter) {
      val result = route(FakeRequest(GET, "/echonr/123"))
      result.map(contentAsString) must beSome("Number: 123")
    }

    "don't resolve route with int part but string in url" in new WithApplication(DynamicTestController.FakeAppWithRouter) {
      val result = route(FakeRequest(GET, "/echonr/hallo"))
      result.map(contentAsString) must beNone
    }

    "resolve route with two dynamic parts" in new WithApplication(DynamicTestController.FakeAppWithRouter) {
      val result = route(FakeRequest(GET, "/both/123/test"))
      result.map(contentAsString) must beSome("123 test")
    }

    "resolve route with two string parts" in new WithApplication(DynamicTestController.FakeAppWithRouter) {
      val result = route(FakeRequest(GET, "/one/hallo/two/welt"))
      result.map(contentAsString) must beSome("hallo welt")
    }
  }

}

