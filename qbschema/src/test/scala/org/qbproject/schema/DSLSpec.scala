package org.qbproject.schema

import org.specs2.mutable.Specification
import play.api.libs.json._
import play.api.libs.json.Json.toJsFieldJsValueWrapper
import scala.math.BigDecimal.long2bigDecimal

object DSLSpec extends Specification {

  import QBSchema._

  "DSL" should {

    val schema = qbClass("time" -> qbPosixTime)

    "have a posix time type" in {
      val currentTime = System.currentTimeMillis() / 1000L
      val instance = Json.obj("time" -> currentTime)
      QBValidator.validate(schema)(instance).get \ "time" must beEqualTo(JsDefined(JsNumber(currentTime)))
    }

    "not validate posix time instances with a double value set" in {
      val instance = Json.obj("time" -> 11.11)
      QBValidator.validate(schema)(instance) must beAnInstanceOf[JsError]
    }

    "not validate posix time instances with a negative value set" in {
      val instance = Json.obj("time" -> -1000)
      QBValidator.validate(schema)(instance) must beAnInstanceOf[JsError]
    }
  }
}