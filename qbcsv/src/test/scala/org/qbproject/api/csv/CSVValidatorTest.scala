package org.qbproject.api.csv

import org.specs2.mutable._
import org.specs2.runner.JUnitRunner
import java.io.ByteArrayInputStream
import play.api.libs.json._
import org.qbproject.schema.{QBClass, QBType, QBValidator, QBSchema}
import QBSchema._
import org.junit.runner.RunWith

@RunWith(classOf[JUnitRunner])
class CSVValidatorTest extends Specification {

  val testSchema = qbClass(
    "id" -> qbString,
    "name" -> qbString,
    "email" -> qbString,
    "age" -> readOnly(qbNumber),
    "tags" -> qbList(qbString))

  val testData = """id;name;email;age;tags[0];tags[1]
    1;Eddy;eddy@qb.org;28;yolo;quake
    2;Otto;otto@qb.org;26;ginger;"""

  "CSV Adapter" should {

    def parse(value: QBType, testData: String, adapter: CSVSchemaAdapter = new CSVSchemaAdapter {}) = {
      val parser = new CSVColumnUtil(row => adapter.adapt(value.asInstanceOf[QBClass])(row))
      parser.parse(new ByteArrayInputStream(testData.getBytes("utf-8")), ';', '"')(identity)
    }

    "read basic csv" in {
      val result = parse(testSchema, testData)
      result.size must beEqualTo(2)
      result(0).get.get must beEqualTo(Json.obj(
        "id" -> "1",
        "name" -> "Eddy",
        "email" -> "eddy@qb.org",
        "age" -> 28,
        "tags" -> Json.arr("yolo", "quake")))
    }

    "read basic csv with default" in {
      val testSchema = qbClass(
        "id" -> qbString,
        "name" -> qbString,
        "email" -> qbString,
        "age" -> readOnly(qbNumber),
        "tags" -> qbList(qbString),
        "sex" -> default(qbString, JsString("f")))
      val result = parse(testSchema, testData)
      result.size must beEqualTo(2)
      val r = QBValidator.validate(testSchema)(result(0).get.get.asInstanceOf[JsObject])
      r.get must beEqualTo(Json.obj(
        "id" -> "1",
        "name" -> "Eddy",
        "email" -> "eddy@qb.org",
        "age" -> 28,
        "tags" -> Json.arr("yolo", "quake"),
        "sex" -> "f"))
    }

    "read basic csv with missing optional" in {
      val testSchema = qbClass(
        "id" -> qbString,
        "name" -> qbString,
        "email" -> qbString,
        "age" -> readOnly(qbNumber),
        "tags" -> qbList(qbString),
        "sex" -> optional(qbString, JsString("f")))
      val testData = """id;name;email;age;tags[0];tags[1];sex
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
        "sex" -> "f"))
    }

    "read basic csv with missing optional and without fallback value" in {
      val testSchema = qbClass(
        "id" -> qbString,
        "name" -> qbString,
        "email" -> qbString,
        "age" -> readOnly(qbNumber),
        "tags" -> qbList(qbString),
        "sex" -> optional(qbString))
      val testData = """id;name;email;age;tags[0];tags[1];sex
        1;Eddy;eddy@qb.org;28;yolo;quake
        2;Otto;otto@qb.org;26;ginger"""
      val result = parse(testSchema, testData)
      result.size must beEqualTo(2)
      QBValidator.validateJsValue(testSchema)(result(0).get.get).get must beEqualTo(Json.obj(
        "id" -> "1",
        "name" -> "Eddy",
        "email" -> "eddy@qb.org",
        "age" -> 28,
        "tags" -> Json.arr("yolo", "quake")))
    }

    "read basic csv with missing optional enum and with fallback value" in {
      val testSchema = qbClass(
        "id" -> qbString,
        "name" -> qbString,
        "email" -> qbString,
        "age" -> readOnly(qbNumber),
        "tags" -> qbList(qbString),
        "sex" -> optional(qbEnum("foo", "bar", "quux"), JsString("bar")))
      val testData = """id;name;email;age;tags[0];tags[1];sex
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
        "sex" -> "bar"))
    }

    "read basic csv with missing optional enum and without fallback value" in {
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
//
    "read basic csv with present optional" in {
      val testSchema = qbClass(
        "id" -> qbString,
        "name" -> qbString,
        "email" -> qbString,
        "age" -> readOnly(qbNumber),
        "sex" -> optional(qbString, JsString("f")),
        "tags" -> qbList(qbString)
        )
      val testData = """id;name;email;age;sex;tags[0];tags[1];
        1;Eddy;eddy@qb.org;28;m;yolo;quake
        2;Otto;otto@qb.org;26;f;ginger"""
      val result = parse(testSchema, testData)
      result.size must beEqualTo(2)
      // TODO: get get get
      QBValidator.validate(testSchema)(result(0).get.get.asInstanceOf[JsObject]).get must beEqualTo(Json.obj(
        "id" -> "1",
        "name" -> "Eddy",
        "email" -> "eddy@qb.org",
        "age" -> 28.0,
        "sex" -> "m",
        "tags" -> Json.arr("yolo", "quake")
        ))
    }

//    "convert path to String" in {
//      resolvePath((JsPath \ "hallo" \ "dude")(1)) must beEqualTo("hallo.dude[1]")
//    }

//    "works for #pathExists" in {
//
//      val row = new CSVRow(List("1", "", ""), List("A", "B"))
//
//      pathExists((JsPath \ "A"))(row) must beTrue
//      pathExists((JsPath \ "B"))(row) must beFalse
//      pathExists((JsPath \ "C"))(row) must beFalse
//    }

    "read csv with boolean" in {
      val data = "bool;\ntrue"
      val schema = qbClass("bool" -> qbBoolean)

      val result = parse(schema, data)

      result must have size 1
      result(0).get.get must beEqualTo(Json.obj(
        "bool" -> true))
    }

    "allow tranformer plugins" in {
      val rangeData = """id;range
         1;"2-3""""
      val rangeSchema = qbClass(
        "id" -> qbNumber,
        "range" -> qbClass(
          "start" -> qbNumber,
          "end" -> qbNumber))

      val range = "([0-9]+)\\s*-\\s*([0-9]+)".r

      val transformer = new CSVAdaptedValidator(List(
        ("range.start", "range", (x: String) => x match { case range(start, end) => start }),
        ("range.end", "range", (x: String) => x match { case range(start, end) => end })))

      val result = parse(rangeSchema, rangeData, transformer)

      result.size must beEqualTo(1)
      result(0).get.get must beEqualTo(Json.obj(
        "id" -> 1,
        "range" -> Json.obj(
          "start" -> 2,
          "end" -> 3)))
    }

    "allow arrays with simple value in one column" in {
      val csv = "array\n\"a\nb\""
      val schema = qbClass(
        "array" -> qbList(qbString))

      def arrayAdapter(index: Int)(value: String): String = {
        value.split("\n")(index)
      }

      // TODO Argh, how ugly -.-
      val transformer = new CSVAdapter(List(
        ("array", "array", (value: String) => value),
        ("array[0]", "array", arrayAdapter(0) _),
        ("array[1]", "array", arrayAdapter(1) _)))

      val result = parse(schema, csv, transformer)

      result must have size 1
      result(0).get.get must beEqualTo(Json.obj(
        "array" -> List("a", "b")))
    }

    "allow arrays with multiple values in one column" in {
      val csv = "array\n\"1//2\n3//4\""
      val schema = qbClass(
        "array" -> qbList(qbClass(
          "first" -> qbString,
          "second" -> qbString)))

      def arrayAdapter(line: Int, part: Int)(value: String) = {
        val results = value.split("\n")(line).split("//").map(_.trim)
        results(part)
      }

      // TODO Argh, how ugly -.-
      val transformer = new CSVAdapter(List(
        ("array", "array", (value: String) => value),
        ("array[0].first", "array", arrayAdapter(0, 0) _),
        ("array[0].second", "array", arrayAdapter(0, 1) _),
        ("array[1].first", "array", arrayAdapter(1, 0) _),
        ("array[1].second", "array", arrayAdapter(1, 1) _)))

      val result = parse(schema, csv, transformer)

      result must have size 1
      result(0).get.get must beEqualTo(Json.obj(
        "array" -> List(Json.obj(
          "first" -> "1",
          "second" -> "2"), Json.obj(
          "first" -> "3",
          "second" -> "4"))))
    }

  }
}