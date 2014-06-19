package org.qbproject.csv

/**
 * Represents a resource mapping statement used within the CSV join specification.
 * A 
 * @param mappings
 */
case class ResourceMapping(mappings: List[MappedResource] = List.empty) {

  def +(singleMapping: (String, ResourceReference)): ResourceMapping = {
    ResourceMapping(MappedResource(singleMapping._1, singleMapping._2) :: mappings)
  }

  def resourceIdentifiers: List[String] = mappings.map(_.resourceRef).map(_.resourceIdentifier).toList

  private def attributesWithJoinKeys: List[(String, JoinKeySpec)] =
    mappings.map(entry => entry.attributeName -> entry.resourceRef.joinKeys).toList


  def allExceptSplitKeys: List[(String, JoinKeySpec)] = attributesWithJoinKeys.collect {
    case a@(_, joinKey) if !joinKey.isInstanceOf[SplitKey] => a
  }

  def foreignSplitKeys: List[(String, ForeignSplitKey)] = attributesWithJoinKeys.collect {
    case (attr, joinKey: ForeignSplitKey) => attr -> joinKey
  }

  def map[A](f: MappedResource => A): List[A] = {
    mappings.map(f)
  }

}

case class MappedResource(attributeName: String, resourceRef: ResourceReference)