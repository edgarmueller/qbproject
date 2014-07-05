package org.qbproject.schema.internal.json.mapper

import play.api.libs.json.{JsString, JsValue}

/**
 * Convenience partial functions that may be passed into the map function of a type mapper.
 */
trait JsTypeMapperOps {

  val trim: PartialFunction[JsValue, JsValue] = {
    case JsString(s) => JsString(s.trim)
  }

  val toLowerCase: PartialFunction[JsValue, JsValue] = {
    case JsString(s) => JsString(s.toLowerCase)
  }

  val toUpperCase: PartialFunction[JsValue, JsValue] = {
    case JsString(s) => JsString(s.toUpperCase)
  }
}
