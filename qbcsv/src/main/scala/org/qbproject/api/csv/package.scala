package org.qbproject.api

import org.qbproject.csv._
import org.qbproject.csv.SplitKey
import org.qbproject.csv.ResourceReference
import org.qbproject.csv.JoinKey
import play.api.libs.json.{JsError, JsValue}

package object csv {

  implicit def resource(resourceIdentifier: String, identicalJoinKey: String): ResourceReference =
    ResourceReference(resourceIdentifier, JoinKey(identicalJoinKey, identicalJoinKey))

  implicit def resource(resourceIdentifier: String, joinKeys: JoinKeySpec): ResourceReference =
    ResourceReference(resourceIdentifier, joinKeys)

  implicit def toPathSpec(specs: (String, Any => JsValue)*): Seq[(PathSpec, Any => JsValue)] = {
    specs.map { spec =>
      if (spec._1.contains("$")) {
        val splitted = spec._1.split('$').toList
        MappedPath(splitted.head, splitted.last) -> spec._2
      } else {
        Path(spec._1) -> spec._2
      }
    }
  }

  implicit class JoinKeyExtensions(joinKey: String) {
    def splitKey = SplitJoinKeyHelper(joinKey)
    def <->(s2: String) = JoinKey(joinKey, s2)
    def <->(s2: SplitJoinKeyHelper) = ForeignSplitKey(joinKey, s2.key)

  }

  implicit class MappedPathStringExtensions(str: String) {
  // TODO: extract constant
    def maps(otherString: String): String = str + "$" + otherString
    def -->(pf: PartialFunction[Any, JsValue]): (PathSpec, PartialFunction[Any, JsValue]) =  if (str.contains("$")) {
        val splitted = str.split('$').toList
        MappedPath(splitted.head, splitted.last) -> pf
      } else {
        Path(str) -> pf
      }
  }

  case class MappedPathString(str: String, otherString: String)

  implicit class SplitJoinKeyExtensions(joinKey: SplitJoinKeyHelper) {
    def <->(s2: String) = SplitKey(joinKey.key, s2)
  }

  case class SplitJoinKeyHelper(key: String)

  case class QBCSVError(msg: String, resourceIdentifier: String, row: Int, header: String)
  case class QBCSVErrorMap(errorMap: Map[String, Seq[QBCSVError]]) {
    def prettyPrint = errorMap.foldLeft(new StringBuilder)((builder, entry) =>
      builder.append("CSV file " + entry._1 + "\n" +
        entry._2.map(error => "\t at row " + error.row + ": " + error.msg).mkString("\n") + "\n"
      )
    )
  }

  object QBCSVErrorMap {
    def apply(error: JsError): QBCSVErrorMap = {
      val errorMap = error.errors.foldLeft(Map[String, List[QBCSVError]]())((errorMap, pathWithErrors) =>
        pathWithErrors._2.foldLeft(errorMap)((map, error) =>
          error.args.toList.foldLeft(map)((map, e) => e match { // TODO: use collect
            case csvError: CSVErrorInfo =>
              val currentErrors = errorMap.getOrElse(csvError.resource, List[QBCSVError]())
              map.updated(csvError.resource, currentErrors :+ QBCSVError(error.message, csvError.resource, csvError.csvRow, csvError.header))
          }
          )
        )
      )
      new QBCSVErrorMap(errorMap)
    }
  }

}
