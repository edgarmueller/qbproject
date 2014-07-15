package org.qbproject.csv.internal

/**
 * Represents a resource mapping statement used within the CSV join specification.
 *
 * @param mappings
 *                 a list of resource mappings
 */
case class ResourceMapping(mappings: List[MappedResource] = List.empty) {

  /**
   * Add a single resource mapping to this map
   *
   * @param singleMapping
   *                      a mapping between an attribute and a resource reference
   * @return a new mapping containing the passed single mapping
   */
  def +(singleMapping: (String, ResourceReference)): ResourceMapping = {
    ResourceMapping(MappedResource(singleMapping._1, singleMapping._2) :: mappings)
  }

  /**
   * Returns all resource identifiers.
   *
   * @return all resource identifiers
   */
  def resourceIdentifiers: List[String] = mappings.map(_.resourceRef).map(_.resourceIdentifier).toList

  private def attributesWithJoinKeys: List[(String, JoinKeySpec)] =
    mappings.map(entry => entry.attributeName -> entry.resourceRef.joinKeys).toList


  /**
   * Returns all attribute names that match the given criteria.
   *
   * @return a list containing the attribute names that matched the criteria
   */
  def getAttributesBy(p: JoinKeySpec => Boolean): List[String] = attributesWithJoinKeys.collect {
    case a@(_, joinKey) if p(joinKey)=> a._1
  }

  /**
   * Returns all reverse split key.
   *
   * @return a list containing reverse split keys only
   */
  def reverseSplitKeys: List[ReverseSplitKey] = attributesWithJoinKeys.collect {
    case (_, joinKey: ReverseSplitKey) => joinKey
  }

  /**
   * Map over the resource mappings.
   *
   * @param f
   *          the mapping function
   * @tparam A
   *          the result type
   * @return
   */
  def map[A](f: MappedResource => A): List[A] = {
    mappings.map(f)
  }

}

/**
 * Represents a mapping between an attribute and a resource reference.
 *
 * @param attributeName
 *                    the name of the attribute into which to inject data
 * @param resourceRef
 *                    the resource reference containing the resource ID and a join specification
 */
case class MappedResource(attributeName: String, resourceRef: ResourceReference)