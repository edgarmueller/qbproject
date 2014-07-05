package org.qbproject.routing

import org.specs2.mutable._
import org.qbproject.routing.{ GET => qbGET, POST => qbPOST, _ }
import play.core.Router
import play.api.mvc._
import play.api.test.WithApplication
import play.api.test.FakeRequest
import play.api.test.PlaySpecification
import play.api.test.FakeApplication
import scala.concurrent._
import ExecutionContext.Implicits.global
import org.qbproject.routing.internal.QBRouterUtil.QBRouteCollector

class QBBaseRouterExtensionsSpec extends PlaySpecification {

  object TestController extends Controller {

    // Actions
    def root = Action { Ok("root") }
    def foo = Action { Ok("foo") }
    def bar = Action { Ok("bar") }

    // Routes (qb Prefixes due to import conflict)
    val rootRoute = GET / "" to root
    val fooRoute = GET / "foo" to foo

    val nsRoute = namespace("ns", fooRoute)
    val nsRoute2 = namespace("ns2", nsRoute)

    val allRoutes = List(nsRoute, nsRoute2)

    val FakeAppWithRouter = new FakeApplication {
      override lazy val routes: Option[Router.Routes] =
        Some(QBRouter(allRoutes))
    }
  }

  "QBBaseRouter with Namespaces" should {

    "resolve route with namespace" in new WithApplication(TestController.FakeAppWithRouter) {
      val result = route(FakeRequest(GET, "/ns/foo"))
      result.map(contentAsString) must beSome("foo")
    }

    "resolve route with two namespaces" in new WithApplication(TestController.FakeAppWithRouter) {
      val result = route(FakeRequest(GET, "/ns2/ns/foo"))
      result.map(contentAsString) must beSome("foo")
    }

  }

  "Route Collector" should {

    "collect a single route" in {
      implicit val collector = QBRouteCollector()

      GET / "bla" to TestController.foo

      collector.routes.length must beEqualTo(1)
      collector.routes(0).path must beEqualTo("/bla")
    }

    "collect two routes" in {
      implicit val collector = QBRouteCollector()

      GET / "bla" to TestController.foo
      GET / "blub" to TestController.foo

      collector.routes.length must beEqualTo(2)
      collector.routes(0).path must beEqualTo("/bla")
      collector.routes(1).path must beEqualTo("/blub")
    }

  }

  object WrappingTestController extends Controller {
    // Actions
    def foo = Action { Ok("foo") }

    val fooRoute = (GET / "foo" to foo).wrapWith(SetHeaderAction(_))
    val authRoute = GET / "auth" / "foo" to foo
    val notWrappedRoute = GET / "bar" to foo

    val FakeAppWithRouter = new FakeApplication {
      override lazy val routes: Option[Router.Routes] = Some(new QBRouter {

        override val qbRoutes =
          fooRoute ::
            authRoute.wrapWith(TestAuthAction(_)) ::
            notWrappedRoute ::
            namespace("ns", fooRoute) :: Nil

      })
    }

    case class SetHeaderAction[A](action: Action[A]) extends Action[A] {
      def apply(request: Request[A]): Future[SimpleResult] = {
        action(request).map(_.withHeaders("foo" -> "bar"))
      }
      def parser = action.parser
    }

    case class TestAuthAction[A](action: Action[A]) extends Action[A] {
      def apply(request: Request[A]): Future[SimpleResult] = {
        if (request.headers.get("auth") == Some("secret")) {
          action(request)
        } else {
          Future.successful(Ok("No Auth!"))
        }
      }
      def parser = action.parser
    }
  }

  "QBRouter with Wrapping" should {

    "return wrapped route" in new WithApplication(WrappingTestController.FakeAppWithRouter) {
      val result = route(FakeRequest(GET, "/foo"))
      result.map(contentAsString) must beSome("foo")
      result.flatMap(header("foo", _)) must beSome("bar")
    }

    "return nonwrapped route when wrappers are registered in router" in new WithApplication(WrappingTestController.FakeAppWithRouter) {
      val result = route(FakeRequest(GET, "/bar"))
      result.map(contentAsString) must beSome("foo")
      result.flatMap(header("foo", _)) must beNone
    }

    "return content of action which blocks initial action" in new WithApplication(WrappingTestController.FakeAppWithRouter) {
      val result = route(FakeRequest(GET, WrappingTestController.authRoute.path))
      result.map(contentAsString) must beSome("No Auth!")
    }

    "wrapping action directs to initial action" in new WithApplication(WrappingTestController.FakeAppWithRouter) {
      val result = route(FakeRequest(GET, WrappingTestController.authRoute.path)
        .withHeaders("auth" -> "secret"))
      result.map(contentAsString) must beSome("foo")
    }

    "not lose wrapper when using namespace (copying)" in new WithApplication(WrappingTestController.FakeAppWithRouter) {
      val result = route(FakeRequest(GET, "/ns/foo"))
      result.map(contentAsString) must beSome("foo")
      result.flatMap(header("foo", _)) must beSome("bar")
    }
  }

}

