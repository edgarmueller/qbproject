package org.qbproject.api

import org.qbproject.csv._
import org.qbproject.csv.SplitJoinKey
import org.qbproject.csv.ResourceReference
import org.qbproject.csv.JoinKey
import play.api.libs.json.JsValue

/**
 * Created by Edgar on 12.06.2014.
 */
package object csv {

  implicit def resource(resourceIdentifier: String, identicalJoinKey: String): ResourceReference =
    ResourceReference(resourceIdentifier, JoinKey(identicalJoinKey, identicalJoinKey))

  implicit def resource(resourceIdentifier: String, joinKeys: JoinKeySpec): ResourceReference =
    ResourceReference(resourceIdentifier, joinKeys)

  implicit def toPathSpec(specs: Seq[(String, PartialFunction[Any, JsValue])]): Seq[(PathSpec, PartialFunction[Any, JsValue])] = {
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
    def <->(s2: SplitJoinKeyHelper) = SplitForeignJoinKey(joinKey, s2.key)

  }

  implicit class MappedPathStringExtensions(str: String) {
  // TODO: extract constant
    def maps(otherString: String): String = str + "$" + otherString
  }

  case class MappedPathString(str: String, otherString: String)

  implicit class SplitJoinKeyExtensions(joinKey: SplitJoinKeyHelper) {
    def <->(s2: String) = SplitJoinKey(joinKey.key, s2)
  }

  case class SplitJoinKeyHelper(key: String)
}
