package org.qbproject.csv

import org.qbproject.api.schema.{QBClass, QBType, QBValidator}
import org.qbproject.csv.internal._
import CSVColumnUtil.CSVRow
import play.api.data.validation.ValidationError
import play.api.libs.json._

class QBCSVValidator(override val pathBuilders: Map[String, CSVRow => JsValue], separator: Char = ';', quote: Char = '"') extends CSVImporter(separator, quote) {

  override protected def internalParse(schema: QBType, resource: QBResource)
                            (joinKeys: Set[ReverseSplitKey]): List[JsResult[JsValue]] = {
    val parser = new CSVValidateRowUtil(schema.asInstanceOf[QBClass])(row => adapt(schema.asInstanceOf[QBClass])(row), joinKeys)
    parser.parse(resource, separator -> quote)
  }

  def validate(schema: QBType, resource: QBResource): Either[String, List[JsValue]] = {
    parse(schema, resource)
  }

  def validate(mainResourceIdentifier: String, schema: QBClass)
                     (resourceMapping: (String, ResourceReference)*)
                     (resourceSet: QBResourceSet): Either[List[QBCSVError], List[JsValue]] = {
    parse(mainResourceIdentifier, schema)(resourceMapping:_*)(resourceSet)
  }

  private class CSVValidateRowUtil(schema: QBClass)(
    factory: CSVRow => JsResult[JsValue], joinKeys: Set[ReverseSplitKey] = Set.empty) extends CSVAdaptRowUtil(factory) {

    override def useParsedResult(jsResult: JsResult[JsValue], csvDiagnosis: CSVDiagnosis) = jsResult.flatMap[JsValue] {
      QBValidator.validateJsValue(schema)(_) match {
        case success @ JsSuccess(_, _) => success
        case _@ JsError(errors) =>
          JsError(errors.map(pathWithErrors => pathWithErrors._1 ->
            pathWithErrors._2.map(error =>
              ValidationError(
                "Validation failed at " +
                  pathWithErrors._1 + ": " +
                  error.message, CSVErrorInfo(csvDiagnosis.resource, csvDiagnosis.row)
              )
            )
          ))
      }
    }
  }

}

object QBCSVValidator {
  def apply(pathConstructors: (PathSpec, Any => JsValue)*) =
    new QBCSVValidator(CSVImporter.toPathBuilders(pathConstructors))
}

