package controllers

import org.joda.time.DateTime
import org.qbproject.mongo._
import org.qbproject.controllers._
import org.qbproject.routing._
import org.qbproject.schema.QBSchema._
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import play.api.libs.json._
import play.api.mvc.{Action, Controller, Request, SimpleResult}
import play.modules.reactivemongo.MongoController

import scala.concurrent.Future

object BlogController extends Controller with MongoController with QBCrudController with QBRouter {

  val blogSchema = qbClass(
    "id" -> objectId,
    "title" -> qbString,
    "body" -> qbString,
    "tags" -> qbList(qbString),
    "author" -> qbString,
    "creationDate" -> qbDateTime)

  //
  lazy val collection = QBMongoDefaultCollection("blog", db, blogSchema)

  override def createSchema = blogSchema -- ("id", "creationDate")

  override def updateSchema = createSchema

//  def createBlogPost = ValidatingAction(blogSchema -- ("id", "creationDate")).async {
//    request =>
//      collection.collection.insert(request.validatedJson).map {
//        error =>
//          if (error.ok) Ok("Blog entry created")
//          else BadRequest(error.message)
//      }
//  }

  def getBlogPostById(blogId: String) = Action.async {
    collection.findById(blogId).map(blog => Ok(Json.toJson(blog.get)))
  }

  def getAllBlogEntries = Action.async {
    collection.all().map(Json.toJson(_)).map(Ok(_))
  }

  override def beforeCreate(blog: JsValue): JsValue =
   blog.asInstanceOf[JsObject] + ("creationDate" -> JsString(new DateTime().toString))


  override val qbRoutes = crudRoutes

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

//object BlogRouter extends QBRouter {
//
//  val getAllRoute =  GET / "all2" -> BlogController.getAllBlogEntries
//  val createRoute = POST / "create2" -> BlogController.create
//
//  override def wrappers = Map(
//    createRoute wrapWith (BlogAuth(_)),
//    BlogController.routes wrapWith (FooAction(_)))
//
//
//  override def qbRoutes = getAllRoute. List(createRoute).wr, getAllRoute) ++ namespace("/api") { BlogController.routes }
//
//}