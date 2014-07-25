package org.qbproject.schema

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.specs2.mutable.Specification
import QBSchema.{ length => strLength, _ }
import play.api.libs.json._
import play.api.libs.json.Json.toJsFieldJsValueWrapper

@RunWith(classOf[JUnitRunner])
class ErrorAggregationSpec extends Specification {

  "Error Aggregation" should {

    "return errors for all fields with empty input object" in {

      val schema = qbClass(
        "ratingStatus" -> qbEnum("pending", "approved", "rejected"),
        "rating" -> qbInteger,
        "comment" -> qbString(maxLength(2000)),
        "firstName" -> optional(qbString(strLength(1, 50))),
        "lastName" -> qbString(strLength(1, 50)),
        "ratingDate" -> qbDateTime)

      val result: JsError = QBValidator.validate(schema)(Json.obj()).asInstanceOf[JsError]
      result.errors.length must beEqualTo(5)
      result.errors(0)._1 must beEqualTo(__ \ "ratingStatus")
      result.errors(1)._1 must beEqualTo(__ \ "rating")
      result.errors(2)._1 must beEqualTo(__ \ "comment")
      // firstname is optional
      result.errors(3)._1 must beEqualTo(__ \ "lastName")
      result.errors(4)._1 must beEqualTo(__ \ "ratingDate")
    }

    "return errors for optional value with wrong type" in {
      val schema = qbClass("name" -> optional(qbString))
      val data = Json.obj("name" -> 1234)

      val result: JsError = QBValidator.validate(schema)(data).asInstanceOf[JsError]
      result.errors(0)._1 must beEqualTo(__ \ "name")
    }

    "return errors for nested objects" in {
      val schema = qbClass(
        "name" -> qbClass(
          "firstName" -> qbString,
          "lastName" -> qbString))
      val data = Json.obj("name" -> Json.obj("firstName" -> "Otto", "lastName" -> 23))

      val result: JsError = QBValidator.validate(schema)(data).asInstanceOf[JsError]

      result.errors.size must beEqualTo(1)
      result.errors(0)._1 must beEqualTo(__ \ "name" \ "lastName")
    }

    "return errors for an array with various instance types (bool)" in {
      val schema = qbClass(
        "a" -> qbList(qbString))
      val data = Json.obj(
        "a" -> Json.arr(true, 2))
      val result = QBValidator.validate(schema)(data).asInstanceOf[JsError]
    // TODO: we get as many error messages as instance types do not fit in the error
      result.errors.size must beEqualTo(2)
      result.errors(0)._2(0).message must contain("qb.incompatible.types")
    }


    "emit error message in case the instance does not adhere the schema" in {
      val schema = qbClass(
        "o" -> qbClass(
          "s" -> qbString(minLength(5))))
      val instance = Json.obj(
        "o" -> Json.obj(
          "s" -> 5))
      val result = QBValidator.validate(schema)(instance)
      result.asOpt.isDefined must beFalse
    }

    "emit error message in case the instance violates the schema [array]" in {
      val schema = qbClass(
        "o" -> qbClass(
          "s" -> qbList(qbString)))
      val instance = Json.obj(
        "o" -> Json.obj(
          "s" -> 5))
      val result = QBValidator.validate(schema)(instance).asInstanceOf[JsError]
      result.errors(0)._2(0).message must beEqualTo("qb.incompatible.types[expected: array, was: number]")
      result.asOpt.isDefined must beFalse
    }

    "emit error message in case the instance violates the schema [string in array]" in {
      val schema = qbClass(
        "o" -> qbClass(
          "s" -> qbList(qbString)))
      val instance = Json.obj(
        "o" -> Json.obj(
          "s" -> List(5)))
      val result = QBValidator.validate(schema)(instance).asInstanceOf[JsError]
      result.errors(0)._2(0).message must beEqualTo("qb.incompatible.types[expected: string, was: number]")
      result.asOpt.isDefined must beFalse
    }

    "return errors for an object with duplicate field names" in {
      qbClass(
        "a" -> qbList(qbInteger),
        "a" -> qbNumber) must throwA[RuntimeException]("qb.duplicate.fields - a")
    }
  }
}