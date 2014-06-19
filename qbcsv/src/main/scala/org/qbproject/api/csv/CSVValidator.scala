package org.qbproject.api.csv

import org.qbproject.api.schema.{QBValidator, QBClass, QBType}
import org.qbproject.api.csv.CSVColumnUtil.CSVRow
import org.qbproject.csv._
import play.api.libs.json._
import play.api.data.validation.ValidationError

object CSVValidator {
  def apply(pathConstructors: (PathSpec, Any => JsValue)*) =
    new CSVValidator(CSVAdapter.toPathBuilders(pathConstructors))
}

class CSVValidator(_pathBuilders: Map[String, CSVRow => JsValue]) extends CSVAdapter(_pathBuilders) {

  override def parse(schema: QBType, resource: QBResource, joinKeys: Set[ForeignSplitKey] = Set.empty): List[JsResult[JsValue]] = {
    val parser = new CSVValidateRowUtil(schema.asInstanceOf[QBClass])(row => adapt(schema.asInstanceOf[QBClass])(row), joinKeys)
    parser.parse(resource, ';', '"')
  }

  class CSVValidateRowUtil(schema: QBClass)(
    factory: CSVRow => JsResult[JsValue], joinKeys: Set[ForeignSplitKey] = Set.empty) extends CSVAdaptRowUtil(factory) {

    override def useParsedResult(jsResult: JsResult[JsValue], csvDiagnosis: CSVDiagnosis) = jsResult.flatMap[JsValue] {
      QBValidator.validateJsValue(schema)(_) match {
        case success@JsSuccess(_, _) => success
        case _@JsError(errors) =>
          JsError(errors.map(pathWithErrors => pathWithErrors._1 ->
            pathWithErrors._2.map(error =>
              ValidationError(
                "Validation failed at " +
                  pathWithErrors._1 + ": " +
                  error.message, CSVErrorInfo(csvDiagnosis.resource, csvDiagnosis.row)))))
      }
    }
  }
}
