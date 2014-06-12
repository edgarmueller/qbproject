package org.qbproject.schema.json

import org.specs2.mutable.Specification
import play.api.libs.json.Json
import play.api.libs.json.Json.toJsFieldJsValueWrapper
import play.api.libs.json._
import org.qbproject.schema._
import org.qbproject.api.schema.{QBClass, QBPartialValidator, QBValidator, QBSchema}
import QBSchema._
import scala.math.BigDecimal.int2bigDecimal
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
object JsValidationSpec extends Specification {

  "QBValidator" should {

    "should accept correct JSON" in {
      val schema = qbClass(
        "o" -> qbClass(
          "s" -> qbString(minLength(5))))
      val instance = Json.obj(
        "o" -> Json.obj(
          "s" -> "hallo"))
      QBValidator.validate(schema)(instance).asOpt.isDefined must beTrue
    }

    "should decline invalid JSON" in {
      val schema = qbClass(
        "o" -> qbClass(
          "s" -> qbString(minLength(5))))
      val instance = Json.obj(
        "o" -> Json.obj(
          "s" -> "hall"))
      QBValidator.validate(schema)(instance).asOpt.isDefined must beFalse
    }

    "should emit same JSON as read if no transformation is present" in {
      val schema = qbClass(
        "o" -> qbString,
        "e" -> qbNumber)
      val instance = Json.obj(
        "o" -> "o",
        "e" -> 23)

      QBValidator.validate(schema)(instance).asOpt.get must beEqualTo(instance)
    }

    "test schema adapter" in {
      val schema = qbClass(
        "o" -> qbString,
        "e" -> qbNumber,
        "d" -> default(qbNumber, JsNumber(42)))
      val instance = Json.obj(
        "o" -> "o",
        "e" -> 23)

      QBValidator.validate(schema)(instance).asOpt.get \ "d" must beEqualTo(JsNumber(42))
    }


    "test optional base type without default" in {
      val schema = qbClass(
        "o" -> qbString,
        "e" -> qbNumber,
        "d" -> optional(qbNumber))
      val instance = Json.obj(
        "o" -> "o",
        "e" -> 23)

      QBValidator.validate(schema)(instance).asOpt.isDefined must beTrue
    }

    "test optional object without default " in {
      val schema = qbClass(
        "o" -> qbString,
        "e" -> qbNumber,
        "d" -> optional(qbClass("i" -> qbString)))
      val instance = Json.obj(
        "o" -> "o",
        "e" -> 23)

      QBValidator.validate(schema)(instance).asOpt.isDefined must beTrue
    }

    "test nested optional object without default " in {
      val schema = qbClass(
        "o" -> qbString,
        "e" -> qbNumber,
        "d" -> optional(qbClass("i" -> optional(qbString))))
      val instance = Json.obj(
        "o" -> "o",
        "e" -> 23)

      QBValidator.validate(schema)(instance).asOpt.isDefined must beTrue
    }

    "test optional without default" in {
      val schema = qbClass(
        "s" -> qbString,
        "n" -> qbNumber,
        "o" -> optional(qbNumber))
      val instance = Json.obj(
        "s" -> "o",
        "n" -> 23)

      QBValidator.validate(schema)(instance).asOpt.isDefined must beTrue
    }

    "test optional without default should not yield JsUndefined" in {
      val schema = qbClass(
        "s" -> qbString,
        "n" -> qbNumber,
        "o" -> optional(qbNumber))
      val instance = Json.obj(
        "s" -> "o",
        "n" -> 23)

      QBValidator.validate(schema)(instance).asOpt must beSome.which(_.fields.size == 2)
    }

    "test optional with already set value" in {
      val schema = qbClass(
        "o" -> qbString,
        "e" -> qbNumber,
        "d" -> optional(qbNumber, JsNumber(11)))

      val instance = Json.obj(
        "o" -> "o",
        "e" -> 23,
        "d" -> 14)

      QBValidator.validate(schema)(instance).asOpt.get \ "d" must beEqualTo(JsNumber(14))
    }

    "test optional with default" in {
      val schema = qbClass(
        "o" -> qbString,
        "e" -> qbNumber,
        "d" -> optional(qbNumber, JsNumber(11)))
      val instance = Json.obj(
        "o" -> "o",
        "e" -> 23)

      val result = QBValidator.validate(schema)(instance)
      result.asOpt.get \ "d" must beEqualTo(JsNumber(11))
    }

    "test optional nested in an optional object where parent is not set" in {
      val schema = qbClass(
        "o" -> qbString,
        "e" -> qbNumber,
        "d" -> optional(qbClass(
          "x" -> optional(qbString)
        )))
      val instance = Json.obj(
        "o" -> "o",
        "e" -> 23)

      QBValidator.validate(schema)(instance).asOpt must beSome
    }

    "test optional nested in an optional object where child is not set" in {
      val schema = qbClass(
        "o" -> qbString,
        "e" -> qbNumber,
        "d" -> optional(qbClass(
          "x" -> optional(qbString)
        )))

      val instance = Json.obj(
        "o" -> "o",
        "e" -> 23,
        "d" -> qbClass())

      QBValidator.validate(schema)(instance).asOpt must beSome
    }

    "test optional when value is null" in {
      val schema = qbClass(
        "o" -> qbString,
        "e" -> optional(qbNumber)
      )

      val instance = JsObject(Seq(
        "o" -> JsString(""),
        "e" -> JsNull
      ))

      val result = QBValidator.validate(schema)(instance)
      result.asOpt must beSome
    }

    "test optional when value is JsUndefined" in {
      val schema = qbClass(
        "o" -> qbString,
        "e" -> optional(qbNumber)
      )

      val instance = JsObject(Seq(
        "o" -> JsString(""),
        "e" -> JsUndefined("")
      ))

      QBValidator.validate(schema)(instance).asOpt must beSome
    }

    "test multipleOf with valid integer" in {
      val schema = qbClass(
        "i" -> qbInteger(multipleOf(3)))
      val instance = Json.obj(
        "i" -> 9)
      val result = QBValidator.validate(schema)(instance)
      result.asOpt must beSome
    }

    "test multipleOf with invalid integer" in {
      val schema = qbClass(
        "i" -> qbInteger(multipleOf(3)))
      val instance = Json.obj(
        "i" -> 10)
      val result = QBValidator.validate(schema)(instance)
      result.asOpt must beNone
    }

    "test parsing a boolean" in {
      val schema = qbClass(
        "bool" -> qbBoolean)
      val input = Json.obj(
        "bool" -> true)

      val result = QBValidator.validate(schema)(input)
      result.asOpt must beEqualTo(Some(input))
    }

    "test oneOf object validation" in {

      val schema = qbClass(
        "i" -> QBSchema.oneOf(
          qbClass("x" -> qbNumber),
          qbClass("x" -> qbString)))

      val result = QBValidator.validate(schema)(Json.obj("i" -> Json.obj("x" -> "wat")))
      result.asOpt must beSome
    }

    "test oneOf object violation" in {
      val o = qbClass(
        "i" -> QBSchema.oneOf(qbClass("x" -> qbNumber), qbClass("x" -> qbString)))
      QBValidator.validate(o)(Json.obj("j" -> Json.obj("xx" -> "wat"))).asOpt must beNone
    }

    "test recursive definition" in {
      lazy val recSchema: QBClass = qbClass(
        "author" -> qbString,
        "message" -> qbString,
        "replies" -> qbList(recSchema))
      val data = Json.obj(
        "author" -> "Dude",
        "message" -> "Hallo?",
        "replies" -> Json.arr(Json.obj(
          "author" -> "Dudezzzz",
          "message" -> "Hi!",
          "replies" -> Json.arr())))

      QBValidator.validate(recSchema)(data).asOpt.isDefined must beTrue
    }

    "test valid minProperties" in {
      val schema = qbClass(List(
        "i" -> qbInteger,
        "j" -> qbInteger),
        minProperties(1))
      val instance = Json.obj(
        "i" -> 9,
        "j" -> 10)
      val result = QBValidator.validate(schema)(instance)
      result.asOpt must beSome
    }

    "test minProperties violation" in {
      val schema = qbClass(List(
        "i" -> qbInteger,
        "j" -> qbInteger),
        minProperties(3))
      val instance = Json.obj(
        "i" -> 9,
        "j" -> 10)
      val result = QBValidator.validate(schema)(instance)
      result.asOpt must beNone
    }

    "test valid maxProperties" in {
      val schema = qbClass(List(
        "i" -> qbInteger,
        "j" -> qbInteger),
        maxProperties(3))
      val instance = Json.obj(
        "i" -> 9,
        "j" -> 10)
      val result = QBValidator.validate(schema)(instance)
      result.asOpt must beSome
    }

    "test maxProperties violation" in {
      val schema = qbClass(List(
        "i" -> qbInteger,
        "j" -> qbInteger),
        maxProperties(1))
      val instance = Json.obj(
        "i" -> 9,
        "j" -> 10)
      val result = QBValidator.validate(schema)(instance)
      result.asOpt must beNone
    }

    "test tolerant numbers conversion" in {
      val schema = qbClass(List(
        "i" -> qbInteger,
        "j" -> qbNumber))
      val instance = Json.obj(
        "i" -> 9,
        "j" -> "10")
      QBValidator.validate(schema)(instance).asOpt must beSome
    }

    "test tolerant numbers conversion with boundaries" in {
      val schema = qbClass(List(
        "i" -> qbInteger,
        "j" -> qbNumber(min(0.0001), max(1000))))
      val instance = Json.obj(
        "i" -> 9,
        "j" -> "10")
      val validatedInstance = QBValidator.validate(schema)(instance).asOpt
      validatedInstance must beSome.which(i => (i \ "j").as[Double] == 10d)
    }

    "test tolerant integer conversion" in {
      val schema = qbClass(List(
        "i" -> qbInteger,
        "j" -> qbInteger))
      val instance = Json.obj(
        "i" -> 9,
        "j" -> "10")
      val validatedInstance = QBValidator.validate(schema)(instance).asOpt
      validatedInstance must beSome.which(i => (i \ "j").as[Double] == 10d)
    }

    "test error case when tolerant number conversion gets non valid number" in {
      val schema = qbClass(List(
        "i" -> qbInteger,
        "j" -> qbNumber))
      val instance = Json.obj(
        "i" -> 9,
        "j" -> "10aaaa")
      QBValidator.validate(schema)(instance).asOpt must beNone
    }

    "test tolerant boolean conversion false" in {
      val schema = qbClass(List(
        "i" -> qbInteger,
        "j" -> qbBoolean))
      val instance = Json.obj(
        "i" -> 9,
        "j" -> "fALSE")
      val result = QBValidator.validate(schema)(instance)
      println(result)
      result.asOpt must beSome
    }

    "test tolerant boolean conversion with true" in {
      val schema = qbClass(List(
        "i" -> qbInteger,
        "j" -> qbBoolean))
      val instance = Json.obj(
        "i" -> 9,
        "j" -> "true")
      QBValidator.validate(schema)(instance).asOpt must beSome
    }

    "test error case when tolerant boolean conversion gets non valid boolean" in {
      val schema = qbClass(List(
        "i" -> qbInteger,
        "j" -> qbBoolean))
      val instance = Json.obj(
        "i" -> 9,
        "j" -> "MaybeTrue")
      QBValidator.validate(schema)(instance).asOpt must beNone
    }

    "test partial validation" in {
      val schema = qbClass(
        "multipleOf3" -> qbInteger(multipleOf(3)),
        "num" -> qbNumber,
        "str" -> qbString)
      val instance = Json.obj(
        "multipleOf3" -> 9)
      val result = QBPartialValidator.validate(schema)(instance)
      result.asOpt must beSome
    }
  }

}