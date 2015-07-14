package org.qbproject.schema.internal.json.processors

import play.api.libs.json._
import play.api.data.validation.ValidationError
import org.qbproject.schema.internal._
import org.qbproject.schema.internal.visitor._
import scalaz.Validation.fromTryCatch
import org.qbproject.schema.{QBInteger, QBNumber, QBType}

/**
 * Type  processor that allow strings to be treated as numbers, if they are parseable according to the schema.
 */
case object JsStringToNumberTypeProcessor extends TypeProcessor {

  //TODO generalize and remove duplicate code
  /**
   * @inheritdoc
   *
   * @param qbType
   *             the matched QB type
   * @param number
   *              the matched JsValue
   * @param path
   *             the current path
   * @return a JsResult containing a result of type O
   */
  def process(qbType: QBType, number: JsValue, path: QBPath): JsResult[JsValue] = {
    qbType match {
      case n: QBNumber => convertToNumber(n, path, number)
      case i: QBInteger => convertToInteger(i, path, number)
      case x => JsError(path.toJsPath, "qb.error.tolerant.number.unmatched" + x + "//" + number)
    }
  }

  private def convertToNumber(qbType: QBNumber, path: QBPath, number: JsValue): JsResult[JsValue] = {
    number match {
      case n: JsNumber => JsSuccess(n)
      case s: JsString =>
        fromTryCatch(s.value.toDouble)
          .leftMap(t => List(ValidationError("qb.invalid.number.format" + ": " + t.getMessage)))
          .flatMap(d => qbType.validate(JsNumber(d)))
          .fold(errors => JsError(Seq(path.toJsPath -> errors)), JsSuccess(_))
      case _ => JsError("qb.error.tolerant.number")
    }
  }

  private def convertToInteger(qbType: QBInteger, path: QBPath, number: JsValue): JsResult[JsValue] = {
    number match {
      case n: JsNumber => JsSuccess(n)
      case s: JsString =>
        fromTryCatch(s.value.toDouble)
          .leftMap(t => List(ValidationError("qb.invalid.number.format" + ": " + t.getMessage)))
          .flatMap(i => qbType.validate(JsNumber(i)))
          .fold(errors => JsError(Seq(path.toJsPath -> errors)), JsSuccess(_))
      case _ => JsError("qb.error.tolerant.int")
    }
  }

}