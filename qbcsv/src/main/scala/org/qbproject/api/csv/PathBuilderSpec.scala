package org.qbproject.api.csv

import org.qbproject.csv.PathSpec
import play.api.libs.json.JsValue

/**
 * Created by Edgar on 12.06.2014.
 */
case class PathBuilderSpec(pathSpec: PathSpec, builder: PartialFunction[Any, JsValue])
