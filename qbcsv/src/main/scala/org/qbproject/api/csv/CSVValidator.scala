package org.qbproject.api.csv

import org.qbproject.api.schema.{QBValidator, QBClass, QBType}
import org.qbproject.csv._
import play.api.libs.json.{JsObject, JsValue, JsResult}
import org.qbproject.csv.ResourceReference
import org.qbproject.csv.SplitForeignJoinKey
import play.api.libs.json.JsObject
import org.qbproject.api.csv.CSVColumnUtil.CSVRow

object CSVValidator {
  def apply(pathConstructors: (PathSpec, Any => JsValue)*) =
    new CSVValidator(CSVAdapter.toPathBuilders(pathConstructors))
}

class CSVValidator(_pathBuilders: Map[String, CSVRow => JsValue]) extends CSVAdapter(_pathBuilders) {

  override def parse(schema: QBClass, resource: QBResource, joinKeys: Set[SplitForeignJoinKey] = Set.empty): List[JsResult[JsValue]] = {
   super.parse(schema, resource, joinKeys).map {
     _.flatMap(obj => QBValidator.validate(schema)(obj.asInstanceOf[JsObject]))
   }
  }

  override def parse(mainResourceIdentifier: String, schema: QBClass)(resourceMapping: (String, ResourceReference)*)(resourceSet: QBResourceSet): JsResult[List[JsValue]] = {
    super.parse(mainResourceIdentifier, schema)(resourceMapping:_*)(resourceSet).flatMap { parsedResults =>
      val validatedResults = parsedResults.map(result => QBValidator.validate(schema)(result.asInstanceOf[JsObject]))
      sequenceJsResults(validatedResults)
    }
  }

}
