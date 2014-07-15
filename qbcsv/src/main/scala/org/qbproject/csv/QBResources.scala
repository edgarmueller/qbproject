package org.qbproject.csv

import java.io.InputStream

import scalaz.Scalaz._
import scalaz._

/**
 * A resource represents one CSV file.
 * 
 * @param identifier name of resource
 * @param inputStream inputstream of the resource
 */
case class QBResource(identifier: String, inputStream: InputStream) {

  def close() = inputStream.close()

  override def toString = identifier
}

/**
 * A set of multiple QBResources.
 *
 * @param resources
 *                  the contents of the resource set
 */
case class QBResourceSet(resources: QBResource*) {

  private lazy val resourceMap = resources.map(
    resource => resource.identifier -> resource
  ).toMap

  def close() = resources foreach { _.close() }

  def get(identifier: String): Validation[NonEmptyList[QBCSVError], QBResource] = {
    resourceMap.get(identifier).fold[Validation[NonEmptyList[QBCSVError], QBResource]] {
      QBCSVResourceError(s"Resource $identifier not found.").failNel
    } {
      _.successNel
    }
  }
}
