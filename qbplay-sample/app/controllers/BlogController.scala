package controllers

import play.api.libs.concurrent.Execution.Implicits.defaultContext
import scala.concurrent.Future
import play.api.mvc.{Request, SimpleResult, Action, Controller}
import play.api.libs.json._
import play.modules.reactivemongo.MongoController
import org.qbproject.api.controllers.{QBCrudController, QBAPIController}
import org.qbproject.api.mongo.{QBCollectionValidation, QBMongoCollection}
import org.qbproject.schema.QBSchema
import QBSchema._
import org.qbproject.api.routing.QBRouterUtil.namespace
import org.qbproject.api.routing.{QBRouter, QBRoute}
import org.qbproject.api.routing.QBRoutes._
import org.joda.time.DateTime
import org.qbproject.api.mongo.MongoSchemaExtensions._

object BlogController extends Controller with MongoController with QBCrudController {

  val blogSchema = qbClass(
    "id" -> objectId,
    "title" -> qbString,
    "body" -> qbString,
    "tags" -> qbList(qbString),
    "author" -> qbString,
    "creationDate" -> qbDateTime)

  //
  lazy val collection = new QBMongoCollection("blog")(db) with QBCollectionValidation {
    /**
     * TODO: MUST be def!
     */
    override def schema = blogSchema
  }

  override def createSchema = blogSchema -- ("id", "creationDate")

  def createBlogPost = ValidatingAction(blogSchema -- ("id", "creationDate")).async {
    request =>
      collection.collection.insert(request.validatedJson).map {
        error =>
          if (error.ok) Ok("Blog entry created")
          else BadRequest(error.message)
      }
  }

  def getBlogPostById(blogId: String) = Action.async {
    collection.getById(blogId).map(blog => Ok(Json.toJson(blog.get)))
  }

  def getAllBlogEntries = Action.async {
    collection.getAll().map(Json.toJson(_)).map(Ok(_))
  }

  override def beforeCreate(blog: JsValue): JsValue =
   blog.asInstanceOf[JsObject] + ("creationDate" -> JsString(new DateTime().toString()))

  // Routes :

  val createBlogPostRoute: QBRoute = POST / root -> createBlogPost
  val routes = List(createBlogPostRoute) ++ crudRoutes

}
case class BlogAuth[A](action: Action[A]) extends Action[A] {
  def apply(request: Request[A]): Future[SimpleResult] = {
    action(request).map {
      result =>
        result.withHeaders("Eddy" -> "da Boss")
    }
  }
  lazy val parser = action.parser
}

case class FooAction[A](action: Action[A]) extends Action[A] {
  def apply(request: Request[A]): Future[SimpleResult] = {
    action(request).map {
      result =>
        result.withHeaders("Otto's" -> "cookin'")
    }
  }
  lazy val parser = action.parser
}

object BlogRouter extends QBRouter {

  val getAllRoute =  GET / "all2" -> BlogController.getAllBlogEntries
  val createRoute = POST / "create2" -> BlogController.createBlogPost

  override def wrappers = Map(
    createRoute wrapWith (BlogAuth(_)),
    BlogController.routes wrapWith (FooAction(_)))


  override def qbRoutes = List(createRoute, getAllRoute) ++ namespace("/api") { BlogController.routes }

}