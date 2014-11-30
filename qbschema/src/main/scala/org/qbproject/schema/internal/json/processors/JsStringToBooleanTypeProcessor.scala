package org.qbproject.schema.internal.json.processors

import org.qbproject.schema.internal.visitor.TypeProcessor
import org.qbproject.schema.internal.visitor.QBPath
import play.api.libs.json._
import play.api.data.validation.ValidationError
import scalaz.{Failure, Success}
import scalaz.Validation.fromTryCatch
import org.qbproject.schema.{QBBoolean, QBType}

case object JsStringToBooleanTypeProcessor extends TypeProcessor {

  override def process(qbType: QBType, input: JsValue, path: QBPath): JsResult[JsValue] = {
    qbType match {
      case b: QBBoolean if !input.isInstanceOf[JsUndefined]=> convertToBoolean(b, path, input)
      case _ => JsError(path.toJsPath, "qb.error.tolerant.boolean.unmatched")
    }
  }

  private def convertToBoolean(qbType: QBBoolean, path: QBPath, input: JsValue): JsResult[JsValue] = {
    input match {
      case b: JsBoolean => JsSuccess(b)
      case s: JsString =>
        fromTryCatch(s.value.toBoolean)
          .leftMap(t => List(ValidationError("qb.invalid.boolean.format: " + t.getMessage)))
          .flatMap(bool => qbType.validate(JsBoolean(bool)))
          .fold(errors => JsError(Seq(path.toJsPath -> errors)), JsSuccess(_))
      case _ => JsError("qb.error.tolerant.boolean")
    }
  }
}
