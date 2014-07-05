package org.qbproject.schema

import play.api.libs.json._


class QBException(message: String, underlying: Throwable) extends RuntimeException(message, underlying) {
  def this() = this("", null)
  def this(message: String) = this(message, null)
  def this(underlying: Throwable) = this(null, underlying)
}

class QBValidationException(message: String, underlying: Throwable, error: Option[JsError] = None) extends QBException(message, underlying) {
  def this() = this("", null)
  def this(message: String) = this(message, null)
  def this(underlying: Throwable) = this(null, underlying)
  def this(error: JsError) = this(Json.prettyPrint(JsError.toFlatJson(error)), null, Some(error))
}