package org.qbproject

import org.qbproject.csv.internal._
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
    def splitKey(implicit separator: Char = ',') = SplitJoinKeyHelper(joinKey)
    def <->(s2: String) = JoinKey(joinKey, s2)
    def <->(s2: SplitJoinKeyHelper) = ReverseSplitKey(joinKey, s2.key)
  }

  // TODO: make token configurable
  val SPLIT_TOKEN = '$'

  implicit class MappedPathStringExtensions(str: String) {
    // TODO: extract constant
    def maps(otherString: String): String = str + SPLIT_TOKEN + otherString
    def -->(pf: PartialFunction[Any, JsValue]): (PathSpec, PartialFunction[Any, JsValue]) = if (str.contains(SPLIT_TOKEN)) {
      val splitted = str.split(SPLIT_TOKEN).toList
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

  type ResourceId = String

  trait QBCSVError {

    def resourceId: ResourceId
    /**
     * Pretty print the error.
     *
     * @return a string containing the pretty printed error message
     */
    def prettyPrint: String
  }

  case class QBCSVResourceError(resourceId: String) extends QBCSVError {
    def prettyPrint = s"$resourceId not found."
  }


  case class QBCSVJoinError(resourceId: ResourceId, msg: String) extends QBCSVError {
    def prettyPrint: String = msg
  }

  /**
   * A CSV error for a single row.
   *
   * @param msg
   *            the error message
   * @param resourceId
   *            the resource that contains the invalid row
   * @param row
   *            the row number starting at 1
   * @param header
   *            the header containing the invalid cell
   */
  case class QBCSVDataError(msg: String, resourceId: String, row: Int, header: String) extends QBCSVError {
    def prettyPrint: String = "\t at row " + row + ": " + msg
  }

  /**
   * A CSV error that aggregates all CSV errors per resource.
   *
   * @param errors
   *            a mapping of resources to their respective errors
   */
  case class QBCSVErrorMap(errors: Map[ResourceId, Seq[QBCSVError]]) {
    def prettyPrint: String = errors.foldLeft(new StringBuilder)((builder, entry) =>
      builder.append("CSV file " + entry._1 + "\n" +
        entry._2.map(_.prettyPrint).mkString("\n") + "\n"
      )
    ).toString()
  }

  object QBCSVErrorMap {

    def convertToCSVDataErrors(error: JsError): List[QBCSVDataError] = {
      error.errors.toList.flatMap(pathWithErrors =>
        pathWithErrors._2.flatMap(error =>
          error.args.toList.collect {
            case csvError: CSVErrorInfo =>
              QBCSVDataError(error.message, csvError.resource, csvError.csvRow, csvError.header)
          })
      )
    }

    /**
     * Create a QBCSVErrorMap based on a JsError.
     *
     * @param error
     *             the JsError containing CSVErrorInfo instances
     * @return a QBCSVErrorMap mapping resources to their errors
     */
    def apply(error: JsError): QBCSVErrorMap = {
      val errorMap = error.errors.foldLeft(Map[String, List[QBCSVError]]())((errorMap, pathWithErrors) =>
        pathWithErrors._2.foldLeft(errorMap)((map, error) =>
          error.args.toList.foldLeft(map)((map, err) => err match {
              case csvError: CSVErrorInfo =>
                val currentErrors = errorMap.getOrElse(csvError.resource, List[QBCSVDataError]())
                map.updated(csvError.resource, currentErrors :+ QBCSVDataError(error.message, csvError.resource, csvError.csvRow, csvError.header))
              case _ => errorMap // ignore all other errors
            }
          )
        )
      )

      new QBCSVErrorMap(errorMap)
    }
  }

}
