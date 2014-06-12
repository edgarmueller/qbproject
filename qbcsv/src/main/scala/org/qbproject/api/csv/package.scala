package org.qbproject.api

import org.qbproject.csv.{JoinKeySpec, SplitJoinKey, JoinKey, ResourceReference}

/**
 * Created by Edgar on 12.06.2014.
 */
package object csv {

  implicit def resource(resourceIdentifier: String, identicalJoinKey: String): ResourceReference =
    ResourceReference(resourceIdentifier, JoinKey(identicalJoinKey, identicalJoinKey))

//  implicit def resource(resourceIdentifier: String, joinKeys: (String, String)): ResourceReference =
//    ResourceReference(resourceIdentifier, JoinKey(joinKeys._1, joinKeys._2))

  implicit def resource(resourceIdentifier: String, joinKeys: JoinKeySpec): ResourceReference =
    ResourceReference(resourceIdentifier, joinKeys)

  implicit class JoinKeyExtensions(joinKey: String) {
    def splitKey = SplitJoinKeyHelper(joinKey)
    def <->(s2: String) = JoinKey(joinKey, s2)
  }

  implicit class SplitJoinKeyExtensions(joinKey: SplitJoinKeyHelper) {
    def <->(s2: String) = SplitJoinKey(joinKey.key, s2)
  }

  case class SplitJoinKeyHelper(key: String)
}
