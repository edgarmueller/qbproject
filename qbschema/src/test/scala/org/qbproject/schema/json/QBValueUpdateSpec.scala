package org.qbproject.schema.internal.json

import org.specs2.mutable.Specification
import org.qbproject.schema.internal._
import org.qbproject.schema._
import QBSchema._
import play.api.libs.json._
import play.api.libs.json.extensions.JsExtensions
import java.util.Date
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.qbproject.schema.internal.json.mapper.JsValueUpdateBuilder
import org.qbproject.schema.QBValueUpdate
import play.api.libs.json.JsString
import play.api.libs.json.JsNumber
import play.api.libs.json.JsObject

object QBValueUpdateSpec extends Specification {

  "Mapping over types" should {

    val schema = qbClass(
      "o" -> qbString,
      "x" -> qbList(qbClass(
        "d" -> qbInteger,
        "e" -> qbInteger)))

    val instance = Json.obj(
      "o" -> "foo",
      "x" -> List(Json.obj(
        "d" -> 4,
        "e" -> 5)))

    "should find all integers" in {
      val schema = qbClass("s" -> qbString, "i" -> qbInteger)
      val instance = Json.obj("s" -> "foo", "i" -> 3)
      val matchedPaths = QBValueUpdate[QBInteger]().matchedPaths(schema)(instance)
      matchedPaths.get.size must beEqualTo(1)
    }

    "find all, also nested, integers" in {
      val schema = qbClass("o" -> qbString, "i" -> qbInteger, "x" -> qbClass("e" -> qbInteger))
      val instance = Json.obj("o" -> "foo", "i" -> 3, "x" -> Json.obj("e" -> 4))
      val matchedPaths = QBValueUpdate[QBInteger]().matchedPaths(schema)(instance)
      matchedPaths.get.size must beEqualTo(2)
      matchedPaths.get(1) must beEqualTo(JsPath() \ "x" \ "e")
      matchedPaths.get(1) must beEqualTo(JsPath() \ "x" \ "e")
    }

    "find integers in an array" in {
      val schema = qbClass("o" -> qbString, "x" -> qbList(qbClass("d" -> qbInteger, "e" -> qbInteger)))
      val instance = Json.obj("o" -> "foo", "x" -> List(Json.obj("d" -> 4, "e" -> 5)))
      val matchedPaths = QBValueUpdate[QBInteger]().matchedPaths(schema)(instance)
      matchedPaths.get.size must beEqualTo(2)
    }

    "find and increment integers in an array" in {

      val schema = qbClass(
        "o" -> qbString,
        "x" -> qbList(
          qbClass("d" -> qbInteger,
            "e" -> qbInteger)))

      val instance = Json.obj(
        "o" -> "foo",
        "x" -> List(Json.obj(
          "d" -> 4,
          "e" -> 5)))

      val matchedPaths = QBValueUpdate[QBInteger]().matchedPaths(schema)(instance)
      val updatedObject = matchedPaths.get.foldLeft(instance)((o, path) => {
        println(o.get(path))
        o.set((path, JsNumber(o.get(path).as[JsNumber].value + 1))).asInstanceOf[JsObject]
      })
      (updatedObject \ "x")(0) \ "d" must beEqualTo(JsNumber(5))
      (updatedObject \ "x")(0) \ "e" must beEqualTo(JsNumber(6))
    }

    "find and increment integers in an array via map" in {
      val updatedObject = QBValueUpdate[QBInteger]().map(schema)(instance) {
        case JsNumber(n) => JsNumber(n + 1)
      }.get

      (updatedObject \ "x")(0) \ "d" must beEqualTo(JsNumber(5))
      (updatedObject \ "x")(0) \ "e" must beEqualTo(JsNumber(6))
    }

    "find and increment integers in an array via builder" in {
      val now = new Date().toString
      val schema = qbClass(
        "o" -> qbString,
        "x" -> qbList(qbClass(
          "d" -> qbDateTime,
          "e" -> qbInteger)))

      val instance = Json.obj(
        "o" -> "foo",
        "x" -> List(Json.obj(
          "d" -> Json.obj("$date" -> now),
          "e" -> 5)))

      val updatedSchema = schema
        .map[QBDateTime](attr => qbClass("$date" -> qbDateTime))

      val builder = new JsValueUpdateBuilder(updatedSchema).byType[QBClass] {
        case o: JsObject if o.fieldSet.exists(_._1 == "$date") => o.fieldSet.find(_._1 == "$date").get._2
        case o => o
      }

      val updatedObject = builder.go(instance)

      (updatedObject \ "x")(0) \ "d" must beEqualTo(JsString(now))
    }

    "find and increment integers in an array via builder directly" in {

      val now = new Date().toString

      val schema = qbClass(
        "x" -> qbList(qbDateTime))

      val instance = Json.obj(
        "x" -> List(Json.obj("$date" -> now)))

      val updatedSchema = schema
        .map[QBDateTime](qbType => qbClass("$date" -> qbDateTime))

      val builder = new JsValueUpdateBuilder(updatedSchema).byType[QBClass] {
        case o: JsObject if o.fieldSet.exists(_._1 == "$date") => o.fieldSet.find(_._1 == "$date").get._2
        case o => o
      }

      val updatedObject = builder.go(instance)

      (updatedObject \ "x")(0) must beEqualTo(JsString(now))
    }

    "find and uppercase all strings via toUpperCase" in {
      val updatedObject = QBValueUpdate[QBString]().map(schema)(instance)(toUpperCase).get
      (updatedObject \ "o") must beEqualTo(JsString("FOO"))
    }

    "find and convert numbers to strings" in {
      val updatedObject = QBValueUpdate[QBInteger]().map(schema)(instance) {
        case JsNumber(n) => JsString(n.intValue().toString)
      }.get

      (updatedObject \ "x")(0) \ "d" must beEqualTo(JsString("4"))
    }

    "find and uppercase all strings and increment all ints via mapping builder" in {
      val mappingBuilder = new JsValueUpdateBuilder(schema).byType[QBString] {
        case JsString(s) => JsString(s.toUpperCase)
      }.byType[QBInteger] {
        case JsNumber(n) => JsNumber(n + 1)
      }
      val updatedObject = mappingBuilder.go(instance)
      (updatedObject \ "o") must beEqualTo(JsString("FOO"))
      (updatedObject \ "x")(0) \ "d" must beEqualTo(JsNumber(5))
      (updatedObject \ "x")(0) \ "e" must beEqualTo(JsNumber(6))
    }
    
    "convert datetime and posixtime dates to string" in {
      val date = new DateTime(2000,1,1,1,1)
      val expected = "01.01.2000"

      val dateString = date.toString()
      val dateMillis = date.getMillis

      val sampleSchema = qbClass(
        "d" -> qbDateTime,
        "e" -> qbPosixTime)

      val sampleJson = Json.obj(
        "d" -> dateString, 
        "e" -> dateMillis)

      val expectedJson = Json.obj(
        "d" -> expected, 
        "e" -> expected)

      def formatDate(date: DateTime) = DateTimeFormat.forPattern("dd.MM.yyyy").print(date)
      val transformer = new JsValueUpdateBuilder(sampleSchema)
        .byType[QBDateTime] {
          case JsString(dateTime) => JsString(formatDate(DateTime.parse(dateTime)))
          case j => j
        }.byType[QBPosixTime] {
          case JsNumber(time) => JsString(formatDate(new DateTime(time.toLong)))
          case j => j
        }

      transformer.go(sampleJson) must beEqualTo(expectedJson)
    } 

    "distinguish posixdates and numbers" in {
      val date = new DateTime(2000,1,1,1,1)
      val dateMillis = date.getMillis

      val sampleSchema = qbClass(
        "e" -> qbPosixTime,
        "n" -> qbNumber)

      val sampleJson = Json.obj(
        "e" -> dateMillis,
        "n" -> 1)

      val expectedJson = Json.obj(
        "e" -> "01.01.2000", 
        "n" -> 2)

      def formatDate(date: DateTime) = DateTimeFormat.forPattern("dd.MM.yyyy").print(date)
      val transformer = new JsValueUpdateBuilder(sampleSchema)
        .byType[QBPosixTime] {
          case JsNumber(time) => JsString(formatDate(new DateTime(time.toLong)))
          case j => j
        }.byType[QBNumber] {
          case JsNumber(num) => JsNumber(num + 1)
          case j => j
        }

      transformer.go(sampleJson) must beEqualTo(expectedJson)
    } 
  }
}