package org.qbproject.api.csv

import java.io.InputStream
import play.api.libs.json.{JsSuccess, JsError, JsResult}

case class QBResource(identifier: String, inputStream: InputStream) {

  def close = inputStream.close()

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

  def close = resources foreach { _.close }

  /**
   *
   * @param identifier
   * @return
   */
  def get(identifier: String): JsResult[QBResource] = {
    resourceMap.get(identifier).fold[JsResult[QBResource]] {
      JsError(s"Resource $identifier not found.")
    } {
      JsSuccess(_)
    }
  }
}
