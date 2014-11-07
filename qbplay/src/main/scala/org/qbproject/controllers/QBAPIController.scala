package org.qbproject.controllers

import play.api.libs.json._
import play.api.mvc._
import scala.concurrent.Future
import play.api.libs.concurrent.Execution.Implicits._
import org.qbproject.schema.{ QBType, QBValidator }

trait QBAPIController extends Controller {

  def ValidatingAction(schema: QBType, validator: QBValidator = QBValidator, beforeValidate: JsValue => JsValue = identity) = new ActionBuilder[ValidatedJsonRequest] {
    def invokeBlock[A](request: Request[A], block: (ValidatedJsonRequest[A]) => Future[Result]) = {
      extractJsonFromRequest(request).fold(noJsonResponse)(json => {
        val updatedJson = beforeValidate(json)
        validator.validate(schema)(updatedJson) match {
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

  def noJsonResponse: Future[Result] = Future(BadRequest(
    Json.toJson(QBAPIStatusMessage("error", "No valid json found."))
  ))
  
  def jsonInvalidResponse(error: JsError): Future[Result] = Future(BadRequest(
    Json.toJson(QBAPIStatusMessage("error", "Json input didn't pass validation", Some(JsError.toFlatJson(error))))
  ))

  /**
   * Set json headers, so that api calls aren't cached. Especially a problem with IE.
   */
  def JsonHeaders(action: EssentialAction): EssentialAction = EssentialAction { requestHeader =>
    action(requestHeader).map(result => result.withHeaders(
      CACHE_CONTROL -> "no-store, no-cache, must-revalidate",
      EXPIRES -> "Sat, 23 May 1987 12:00:00 GMT",
      PRAGMA -> "no-cache",
      CONTENT_TYPE -> "application/json; charset=utf-8"))
  }
}

case class QBAPIStatusMessage(status: String, message: String = "", details: Option[JsValue] = None)
object QBAPIStatusMessage {
	implicit val format: Format[QBAPIStatusMessage] = Json.format[QBAPIStatusMessage]
}

class ValidatedJsonRequest[A](
  val validatedJson: JsValue,
  val schema: QBType,
  request: Request[A])
  extends WrappedRequest(request)
