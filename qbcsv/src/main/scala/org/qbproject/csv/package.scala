package org.qbproject

import play.api.libs.json.JsValue

package object csv {

  case class JoinData(attributeName: String, keys: JoinKeySpec, data: List[JsValue])
  case class CSVErrorInfo(resource: String, csvRow: Int, header: String = "")
  case class ResourceReference(resourceIdentifier: String, joinKeys: JoinKeySpec)

  /**
   * Join keys --
   */
  trait JoinKeySpec {

    def key: String

    def foreignKey: String

    def toTuple: (String, String) = (key, foreignKey)
  }

  case class JoinKey(key: String, foreignKey: String) extends JoinKeySpec
  case class SplitKey(key: String, foreignKey: String) extends JoinKeySpec
  case class ForeignSplitKey(key: String, foreignKey: String) extends JoinKeySpec

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
