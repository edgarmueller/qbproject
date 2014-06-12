package org.qbproject.csv

case class ResourceMapping(map: Map[String, ResourceReference] = Map.empty) {

  def +(t: (String, ResourceReference)): ResourceMapping = {
    ResourceMapping(map + t)
  }

  def attributes: Iterable[String] = map.keys

  def resourceIdentifiers: List[String] = map.values.map(_.resourceIdentifier).toList

  def attributesWithJoinKeys: List[(String, JoinKeySpec)] = map.map(entry => entry._1 -> entry._2.joinKeys).toList

  def all = map
}