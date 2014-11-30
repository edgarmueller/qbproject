package org.qbproject.mongo

import org.qbproject.schema._
import org.qbproject.schema.QBSchema._
import org.specs2.mutable.Specification
import play.api.libs.json._

object SchemaExtensionSpec extends Specification {

  class QBImage(rules: Set[ValidationRule[JsString]]) extends QBStringImpl(rules)
  def image = new QBImage(Set.empty)

  "Schema extension test" should {
    "allow to map extension types" in {
      val schema = qbClass("img" -> image)
      val instance = Json.obj("img" -> "otto.png")

      val isQBImage = (qbType: QBType) => qbType.isInstanceOf[QBImage]

      schema.transform(instance)(
        isQBImage -> { case JsString(path) => JsString("public/images/" + path) }
      ) must beEqualTo(Json.obj("img" -> "public/images/otto.png"))
    }
  }
}
