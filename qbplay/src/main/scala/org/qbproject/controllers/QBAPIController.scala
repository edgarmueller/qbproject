package org.qbproject.controllers

import play.api.libs.json._
import play.api.mvc._
import scala.concurrent.Future
import play.api.libs.concurrent.Execution.Implicits._
import play.api.http.HeaderNames._
import org.qbproject.schema.{ QBType, QBValidator }

trait QBAPIController extends Controller {

  def ValidatingAction(schema: QBType, validator: QBValidator = QBValidator, beforeValidate: JsValue => JsValue = identity) = new ActionBuilder[ValidatedJsonRequest] {
    def invokeBlock[A](request: Request[A], block: (ValidatedJsonRequest[A]) => Future[SimpleResult]) = {
      extractJsonFromRequest(request).fold(noJsonResponse)(json => {
        val updatedJson = beforeValidate(json)
        validator.validateJsValue(schema)(updatedJson) match {
          case JsSuccess(validatedJson, path) => block(new ValidatedJsonRequest(updatedJson, schema, request))
          case error: JsError => jsonInvalidResponse(error)
        }
      })
    }
  }

  def extractJsonFromRequest[A](implicit request: Request[A]): Option[JsValue] = request.body match {
    case body: play.api.mvc.AnyContent if body.asJson.isDefined => Some(body.asJson.get)
    case body: play.api.libs.json.JsValue => Some(body)
    case _ => None
  }

  // --

  def noJsonResponse: Future[SimpleResult] = Future(BadRequest(
    Json.toJson(QBAPIStatusMessage("error", "No valid json found."))
  ))
  
  def jsonInvalidResponse(error: JsError): Future[SimpleResult] = Future(BadRequest(
    Json.toJson(QBAPIStatusMessage("error", "Json input didn't pass validation", Some(JsError.toFlatJson(error))))
  ))


}

case class QBAPIStatusMessage(status: String, message: String = "", details: Option[JsValue] = None)
object QBAPIStatusMessage {
	implicit val format: Format[QBAPIStatusMessage] = Json.format[QBAPIStatusMessage]
}
class ValidatedJsonRequest[A](
  val validatedJson: JsValue,
  val schema: QBType,
  request: Request[A])
  extends WrappedRequest[A](request)

/**
 * Set json headers, so that api calls aren't cached. Especially a problem with IE.
 */
case class JsonHeaders[A](action: Action[A]) extends Action[A] {
  def apply(request: Request[A]): Future[SimpleResult] = {
    action(request).map(result => result.withHeaders(
      CACHE_CONTROL -> "no-store, no-cache, must-revalidate",
      EXPIRES -> "Sat, 23 May 1987 12:00:00 GMT",
      PRAGMA -> "no-cache",
      CONTENT_TYPE -> "application/json; charset=utf-8"))
  }
  lazy val parser = action.parser
}
