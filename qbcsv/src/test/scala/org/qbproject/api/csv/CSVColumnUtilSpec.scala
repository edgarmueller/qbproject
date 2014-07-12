package org.qbproject.api.csv

import java.io.ByteArrayInputStream

import org.junit.runner.RunWith
import org.qbproject.api.schema.QBSchema._
import org.qbproject.api.schema.{QBClass, QBType, QBValidator}
import org.qbproject.csv._
import org.qbproject.csv.internal.{CSVColumnUtil, CSVImporter, CSVSchemaAdapter}
import org.specs2.mutable._
import org.specs2.runner.JUnitRunner
import play.api.libs.json._

@RunWith(classOf[JUnitRunner])
class CSVColumnUtilSpec extends Specification {

  val testSchema = qbClass(
    "id" -> qbString,
    "name" -> qbString,
    "email" -> qbString,
    "age" -> readOnly(qbNumber),
    "tags" -> qbList(qbString))

  val testData = """id;name;email;age;tags[0];tags[1]
    1;Eddy;eddy@qb.org;28;yolo;quake
    2;Otto;otto@qb.org;26;ginger;"""

  def jsonObj(schema: QBClass, fields: List[String]): JsObject = {
    JsObject(schema.attributes.map(_.name).zip(fields.map(JsString)))
  }

  def jsonObj(schema: QBClass, fields: List[String], fieldModifier: String => JsValue): JsObject = {
    JsObject(schema.attributes.map(_.name).zip(fields.map(fieldModifier)))
  }

  "CSVColumnUtil" should {

    def parse(value: QBType, testData: String, adapter: CSVSchemaAdapter = new CSVSchemaAdapter {}) = {
      val parser = new CSVColumnUtil(row => adapter.adapt(value.asInstanceOf[QBClass])(row))
      val resource = QBResource("virtual", new ByteArrayInputStream(testData.getBytes("utf-8")))
      parser.parse(resource.inputStream)(identity)
    }

    "parse basic CSV" in {
      val result = parse(testSchema, testData)
      result.size must beEqualTo(2)
      result(0).get.get must beEqualTo(Json.obj(
        "id" -> "1",
        "name" -> "Eddy",
        "email" -> "eddy@qb.org",
        "age" -> 28,
        "tags" -> Json.arr("yolo", "quake"))
      )
    }

    "parse basic CSV with default annotation" in {
      val testSchema = qbClass(
        "id" -> qbString,
        "name" -> qbString,
        "email" -> qbString,
        "age" -> readOnly(qbNumber),
        "tags" -> qbList(qbString),
        "gender" -> default(qbString, JsString("m")))
      val result = parse(testSchema, testData)
      result.size must beEqualTo(2)
      val r = QBValidator.validate(testSchema)(result(0).get.get.asInstanceOf[JsObject])
      r.get must beEqualTo(Json.obj(
        "id" -> "1",
        "name" -> "Eddy",
        "email" -> "eddy@qb.org",
        "age" -> 28,
        "tags" -> Json.arr("yolo", "quake"),
        "gender" -> "m")
      )
    }

    "parse basic CSV with missing optional instance value" in {
      val testSchema = qbClass(
        "id" -> qbString,
        "name" -> qbString,
        "email" -> qbString,
        "age" -> readOnly(qbNumber),
        "tags" -> qbList(qbString),
        "gender" -> optional(qbString, JsString("m")))
      val testData = """id;name;email;age;tags[0];tags[1];gender
        1;Eddy;eddy@qb.org;28;yolo;quake
        2;Otto;otto@qb.org;26;ginger"""
      val result = parse(testSchema, testData)

      result.size must beEqualTo(2)
      // TODO: get get get
      QBValidator.validate(testSchema)(result(0).get.get.asInstanceOf[JsObject]).get must beEqualTo(Json.obj(
        "id" -> "1",
        "name" -> "Eddy",
        "email" -> "eddy@qb.org",
        "age" -> 28.0,
        "tags" -> Json.arr("yolo", "quake"),
        "gender" -> "m")
      )
    }

    "parse basic CSV with missing optional instance value and without fallback value" in {
      val testSchema = qbClass(
        "id" -> qbString,
        "name" -> qbString,
        "email" -> qbString,
        "age" -> readOnly(qbNumber),
        "tags" -> qbList(qbString),
        "gender" -> optional(qbString)
      )
      val testData = """id;name;email;age;tags[0];tags[1];gender
        1;Eddy;eddy@qb.org;28;yolo;quake
        2;Otto;otto@qb.org;26;ginger"""
      val result = parse(testSchema, testData)
      result.size must beEqualTo(2)
      QBValidator.validateJsValue(testSchema)(result(0).get.get).get must beEqualTo(Json.obj(
        "id" -> "1",
        "name" -> "Eddy",
        "email" -> "eddy@qb.org",
        "age" -> 28,
        "tags" -> Json.arr("yolo", "quake"))
      )
    }

    "parse basic CSV with missing optional enum value but with fallback value" in {
      val testSchema = qbClass(
        "id" -> qbString,
        "name" -> qbString,
        "email" -> qbString,
        "age" -> readOnly(qbNumber),
        "tags" -> qbList(qbString),
        "gender" -> optional(qbEnum("foo", "bar", "quux"), JsString("bar"))
      )
      val testData = """id;name;email;age;tags[0];tags[1];gender
        1;Eddy;eddy@qb.org;28;yolo;quake
        2;Otto;otto@qb.org;26;ginger"""
      val result = parse(testSchema, testData)
      result.size must beEqualTo(2)
      QBValidator.validateJsValue(testSchema)(result(0).get.get).get must beEqualTo(Json.obj(
        "id" -> "1",
        "name" -> "Eddy",
        "email" -> "eddy@qb.org",
        "age" -> 28,
        "tags" -> Json.arr("yolo", "quake"),
        "gender" -> "bar")
      )
    }

    "parse basic CSV with missing optional enum value and without fallback value" in {
      val testSchema = qbClass(
        "id" -> qbString,
        "enum" -> optional(qbEnum("foo", "bar", "quux")))
      val testData = """id;enum
        1;
        2;foo"""
      val result = parse(testSchema, testData)
      result.size must beEqualTo(2)
      QBValidator.validateJsValue(testSchema)(result(0).get.get).get must beEqualTo(Json.obj(
        "id" -> "1"
      ))
      QBValidator.validateJsValue(testSchema)(result(1).get.get).get must beEqualTo(Json.obj(
        "id" -> "2",
        "enum" -> "foo"
      ))
    }


    "allow range expressions in columns" in {
      val rangeData = """id;range
         1;"2-3""""
      val rangeClass = qbClass(
        "start" -> qbNumber,
        "end" -> qbNumber
      )
      val rangeSchema = qbClass(
        "id" -> qbNumber,
        "range" -> rangeClass
      )

      val rangeRegex = "([0-9]+)\\s*-\\s*([0-9]+)".r
      val adapter = CSVImporter("range" --> { case rangeRegex(start, end) => Json.obj("start" -> start.toInt, "end" -> end.toInt) })
      val result = parse(rangeSchema, rangeData, adapter)

      result.size must beEqualTo(1)
      result(0).get.get must beEqualTo(Json.obj(
        "id" -> 1,
        "range" -> Json.obj(
          "start" -> 2,
          "end" -> 3
        )
      ))
    }


    "parse basic CSV with present optional" in {
      val testSchema = qbClass(
        "id" -> qbString,
        "name" -> qbString,
        "email" -> qbString,
        "age" -> readOnly(qbNumber),
        "gender" -> optional(qbString, JsString("f")),
        "tags" -> qbList(qbString)
        )
      val testData = """id;name;email;age;gender;tags[0];tags[1];
        1;Eddy;eddy@qb.org;28;m;yolo;quake
        2;Otto;otto@qb.org;26;f;ginger"""
      val result = parse(testSchema, testData)
      result.size must beEqualTo(2)

      QBValidator.validate(testSchema)(result(0).get.get.asInstanceOf[JsObject]).get must beEqualTo(Json.obj(
        "id" -> "1",
        "name" -> "Eddy",
        "email" -> "eddy@qb.org",
        "age" -> 28.0,
        "gender" -> "m",
        "tags" -> Json.arr("yolo", "quake")
        )
      )
    }

    "parse CSV with boolean" in {
      val data = "bool;\ntrue"
      val schema = qbClass("bool" -> qbBoolean)

      val result = parse(schema, data)

      result must have size 1
      result(0).get.get must beEqualTo(Json.obj(
        "bool" -> true)
      )
    }
  }
}