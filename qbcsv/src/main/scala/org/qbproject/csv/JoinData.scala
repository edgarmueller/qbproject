package org.qbproject.csv

import play.api.libs.json.JsValue

case class JoinData(attributeName: String, keys: JoinKeySpec, data: List[JsValue])
