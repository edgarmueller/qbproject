package org.qbproject.schema.internal.json

import play.api.libs.json.{JsNumber, JsString, JsValue}

/**
 * Convenience partial functions that may be passed into the map function of a type mapper.
 */
trait JsValueUpdateOps {

  val trim: PartialFunction[JsValue, JsValue] = {
    case JsString(s) => JsString(s.trim)
  }

  val toLowerCase: PartialFunction[JsValue, JsValue] = {
    case JsString(s) => JsString(s.toLowerCase)
  }

  val toUpperCase: PartialFunction[JsValue, JsValue] = {
    case JsString(s) => JsString(s.toUpperCase)
  }

  def inc(i: Int): PartialFunction[JsValue, JsValue] = {
    case JsNumber(n) => JsNumber(n + i)
  }
}
