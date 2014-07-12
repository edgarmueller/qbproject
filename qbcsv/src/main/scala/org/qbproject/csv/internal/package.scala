package org.qbproject.csv

import play.api.libs.json.JsValue

package object internal {

  /**
   * Represents a CSV error internally.
   *
   * @param resource
   *               the resource containing the error
   * @param csvRow
   *               the number of the row containing the error
   * @param header
   *               the header column containing the error
   */
  case class CSVErrorInfo(resource: String, csvRow: Int, header: String = "")

  /**
   * Utility class to capture information about how and with which data
   * to perform a join.
   *
   * @param attributeName
   *             an attribute name that is used as the target for the data to be joined
   * @param keys
   *             specifies how the join should be handled
   * @param data
   *             the data that will be injected
   */
  case class JoinData(attributeName: String, keys: JoinKeySpec, data: List[JsValue])

  /**
   * A resource reference containing a resource identifier and a join key specification.
   *
   * @param resourceIdentifier
   *                 the ID of the resource
   * @param joinKeys
   *                 a join key specification indicating how rows should be joined
   */
  case class ResourceReference(resourceIdentifier: String, joinKeys: JoinKeySpec)

  /**
   * Join keys --
   */
  trait JoinKeySpec {

    def primaryKey: String

    def secondaryKey: String

    def toTuple: (String, String) = (primaryKey, secondaryKey)
  }

  /**
   * Join key specification for an one-to-one mapping.
   *
   * @param primaryKey
   *                   an attribute within the primary resource
   * @param secondaryKey
   *                   an attribute within the secondary resource
   */
  case class JoinKey(primaryKey: String, secondaryKey: String) extends JoinKeySpec

  /**
   * Splits the primary key while joining.
   *
   * @param primaryKey
   *                   an attribute within the primary resource
   * @param secondaryKey
   *                   an attribute within the secondary resource
   */
  case class SplitKey(primaryKey: String, secondaryKey: String) extends JoinKeySpec

  /**
   * Splits the secondary key while joining.
   *
   * @param primaryKey
   *                   an attribute within the primary resource
   * @param secondaryKey
   *                   an attribute within the secondary resource
   */
  case class ReverseSplitKey(primaryKey: String, secondaryKey: String) extends JoinKeySpec

  /**
   * Path specs --
   */
  trait PathSpec {
    def schemaPath: String
    def csvPath: String
  }

  case class Path(path: String) extends PathSpec {
    def schemaPath = path
    def csvPath = path
  }

  case class MappedPath(schemaPath: String, csvPath: String) extends PathSpec

}
