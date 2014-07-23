package org.qbproject.mongo

import org.specs2.mutable.Specification
import org.qbproject.schema._
import org.qbproject.schema.{ValidationRule, QBStringImpl, QBValueUpdate}
import org.qbproject.schema.QBSchema._
import play.api.libs.json._
import play.api.libs.json.Json.toJsFieldJsValueWrapper

object SchemaExtensionSpec extends Specification {

  class QBImage(rules: Set[ValidationRule[JsString]]) extends QBStringImpl(rules)
  def image = new QBImage(Set.empty)

  "Schema extension test" should {
    "allow to map extension types" in {
      val schema = qbClass("img" -> image)
      val instance = Json.obj("img" -> "otto.png")

      QBValueUpdate[QBImage]().map(schema)(instance) {
        case JsString(path) => JsString("public/images/" + path)
      }.get must beEqualTo(Json.obj("img" -> "public/images/otto.png"))
    }
  }
}