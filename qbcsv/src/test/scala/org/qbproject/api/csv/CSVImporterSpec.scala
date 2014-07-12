package org.qbproject.api.csv

import java.io.ByteArrayInputStream
import org.qbproject.api.schema.QBClass
import org.qbproject.csv.internal.CSVImporter
import play.api.libs.json._
import play.api.libs.json.JsArray
import play.api.libs.json.JsString
import org.specs2.mutable.Specification
import org.qbproject.csv._
import org.qbproject.api.schema.QBSchema._

object CSVImporterSpec extends Specification {

  "CSVImporter" should {

    def mkInputStream(data: String) = new ByteArrayInputStream(data.getBytes("UTF-8"))

    object Data {

      trait _basic {

        val companyData = """id;company.name;company.openHours
            1;Dude GmbH;14-18
            2;Nerd Inc.;22-24""".stripMargin

        val featureData = """id;strength;speed
            1;3;2
            2;2;4""".stripMargin

        val productData = """id;name;price
            1;Beer;5
            1;Pizza;8
            2;Hacking;100""".stripMargin
      }

      val basic = new _basic {}

      /**
       * Extended data
       */
      object extended extends _basic {

        override val companyData = """id;company.name;company.openHours;company.employees
            1;Dude GmbH;14-18;e1,e2
            2;Nerd Inc.;12-24;e1,e2,e3""".stripMargin

        val employeeData = """eId;name;tags[0];tags[1]
            e1;Eddy;dev;
            e2;Otto;dev;ginger
            e3;Max;senior architect;""".stripMargin

        override val productData = """id;name;price;range
            1;Beer;5;2-3
            1;Pizza;8;4-5
            2;Hacking;100;6-99""".stripMargin

        val multiProductData = """id;name;price
            1,2;Beer;5
            1;Pizza;8
            2;Hacking;100""".stripMargin
      }

    }

    object Schemas {

      trait _basic {
        val companyCoreSchema = qbClass(
          "name" -> qbString,
          "openHours" -> qbString) // TODO: provide a range type

        val featureSchema = qbClass(
          "strength" -> qbInteger,
          "speed" -> qbInteger
        )

        val productSchema = qbClass(
          "name" -> qbString,
          "price" -> qbNumber
        )

        def companySchema = qbClass(
          "id" -> qbInteger,
          "company" -> companyCoreSchema,
          "features" -> qbList(featureSchema),
          "products" -> qbList(productSchema)
        )
      }

      val basic = new _basic {}

      object extended extends _basic {

        val rangeClass = qbClass(
          "start" -> qbNumber,
          "end" -> qbNumber
        )

        val rangeSchema = qbClass(
          "id" -> qbNumber,
          "range" -> rangeClass
        )

        val employeeSchema = qbClass(
          "eId" -> qbString,
          "name" -> qbString,
          "tags" -> qbList(qbString)
        )

        override def companySchema = qbClass(
          "id" -> qbInteger,
          "company" -> companyCoreSchema,
          "features" -> qbList(featureSchema),
          "products" -> qbList(productSchema),
          "employees" -> qbList(employeeSchema)
        )

        override val productSchema = qbClass(
          "name" -> qbString,
          "price" -> qbNumber,
          "range" -> rangeClass
        )
      }
    }

    // TODO: duplicate method
    def jsonObj(schema: QBClass, fields: List[String]): JsObject = {
      JsObject(schema.attributes.map(_.name).zip(fields.map(JsString)))
    }

    /**
     * Tests ---
     */

    "perform basic joins" in {
      val companies = CSVImporter().parse(
        Schemas.basic.companySchema keep ("id", "company"),
        QBResource("companies.csv", mkInputStream(Data.basic.companyData))
      )
      val firstCompany = companies.right.get(0)
      firstCompany must beEqualTo(Json.obj(
        "id" -> 1,
        "company" -> Json.obj(
          "name" -> "Dude GmbH",
          "openHours" -> "14-18"
        )
      ))
    }

    "allow joins on multi attributes" in {
      val rangeRegex = "([0-9]+)\\s*-\\s*([0-9]+)".r

      val companyResource = QBResource("companies.csv", mkInputStream(Data.extended.companyData))
      val featureResource  = QBResource("features.csv", mkInputStream(Data.basic.featureData))
      val productsResource  = QBResource("products.csv", mkInputStream(Data.extended.productData))
      val employeeResource  = QBResource("employees.csv", mkInputStream(Data.extended.employeeData))
      val resourceSet = QBResourceSet(companyResource, featureResource, productsResource, employeeResource)

      val jsResults = CSVImporter(
        "range"--> {
          case rangeRegex(start, end) => Json.obj("start" -> start.toInt, "end" -> end.toInt)
        },
        "employees".maps("company.employees") --> {
          case employees: String => JsArray(employees.split(",").toList.map(JsString))
        }).parse("companies.csv", Schemas.extended.companySchema)(
          "features" -> resource("features.csv", "id"),
          "products" -> resource("products.csv", "id"),
          "employees" -> resource("employees.csv", "employees".splitKey <-> "eId")
        )(resourceSet)

      jsResults.right.get(0) must beEqualTo(Json.obj(
        "id" -> 1.0,
        "company" -> Json.obj(
          "name" -> "Dude GmbH",
          "openHours" -> "14-18"
        ),
        "employees" -> Json.arr(
          Json.obj(
            "name" -> "Eddy",
            "tags" -> Json.arr("dev", "")
          ),
          Json.obj(
            "name" -> "Otto",
            "tags" -> Json.arr("dev", "ginger")
          )
        ),
        "features" -> Json.arr(
          Json.obj(
            "strength" -> 3.0,
            "speed" -> 2.0
          )
        ),
        "products" -> Json.arr(
          Json.obj(
            "name" -> "Beer",
            "price" -> 5.0,
            "range" -> Json.obj(
              "start" -> 2,
              "end" -> 3
            )
          ),
          Json.obj(
            "name" -> "Pizza",
            "price" -> 8.0,
            "range" -> Json.obj(
              "start" -> 4,
              "end" -> 5
            )
          )
        )
      ))

      jsResults.right.get(1) must beEqualTo(Json.obj(
        "id" -> 2.0,
        "company" -> Json.obj(
          "name" -> "Nerd Inc.",
          "openHours" -> "12-24"
        ),
        "employees" -> Json.arr(
          Json.obj(
            "name" -> "Eddy",
            "tags" -> Json.arr("dev", "")
          ),
          Json.obj(
            "name" -> "Otto",
            "tags" -> Json.arr("dev", "ginger")
          ),
          Json.obj(
            "name" -> "Max",
            "tags" -> Json.arr("senior architect", "" )
          )
        ),
        "features" -> Json.arr(
          JsObject(Seq(
            "strength" -> JsNumber(2.0),
            "speed" -> JsNumber(4.0)
          ))
        ),
        "products" -> Json.arr(
          Json.obj(
            "name" -> "Hacking",
            "price" -> 100.0,
            "range" -> Json.obj(
              "start" -> 6,
              "end" -> 99
            )
          )
        )
      ))
    }

    "allow joins on multi attributes in reverse direction" in {
      val rangeRegex = "([0-9]+)\\s*-\\s*([0-9]+)".r

      val companyResource = QBResource("companies.csv", mkInputStream(Data.extended.companyData))
      val featureResource = QBResource("features.csv", mkInputStream(Data.basic.featureData))
      val productsResource = QBResource("products.csv", mkInputStream(Data.extended.multiProductData))
      val resourceSet = QBResourceSet(companyResource, featureResource, productsResource)

      val result = CSVImporter(
        "range" --> {
          case rangeRegex(start, end) => Json.obj("start" -> start.toInt, "end" -> end.toInt)
        }
      ).parse("companies.csv", Schemas.basic.companySchema)(
          "features" -> resource("features.csv", "id"),
          "products" -> resource("products.csv", "id" <-> "id".splitKey)
        )(resourceSet)

      result.right.get(0) must beEqualTo(
        Json.obj(
          "id" -> 1.0,
          "company" -> Json.obj(
            "name" -> "Dude GmbH",
            "openHours" -> "14-18"
          ),
          "features" -> Json.arr(
            Json.obj(
              "strength" -> 3.0,
              "speed" -> 2.0
            )
          ),
          "products" -> Json.arr(
            Json.obj(
              "name" -> "Beer",
              "price" -> 5.0
            ),
            Json.obj(
              "name" -> "Pizza",
              "price" -> 8.0
            )
          )
        )
      )

      result.right.get(1) must beEqualTo(
        Json.obj(
          "id" -> 2.0,
          "company" -> Json.obj(
            "name" -> "Nerd Inc.",
            "openHours" -> "12-24"
          ),
          "features" -> Json.arr(
            Json.obj(
              "strength" -> 2.0,
              "speed" -> 4.0
            )
          ),
          "products" -> Json.arr(
            Json.obj(
              "name" -> "Beer",
              "price" -> 5.0
            ),
            Json.obj(
              "name" -> "Hacking",
              "price" -> 100
            )
          )
        )
      )
    }

    "report errors if CSV does not conform to schema" in {
      val rangeRegex = "([0-9]+)\\s*-\\s*([0-9]+)".r

      val companyData = """id;company.name;company.openHours
            1;Dude GmbH;14-18
            2xxx;Nerd Inc.;12-24""".stripMargin

      val companyResource = QBResource("companies.csv", mkInputStream(companyData))
      val featureResource = QBResource("features.csv", mkInputStream(Data.basic.featureData))
      val productsResource = QBResource("products.csv", mkInputStream(Data.extended.multiProductData))
      val resourceSet = QBResourceSet(companyResource, featureResource, productsResource)

      val result = CSVImporter(
        "range" --> {
          case rangeRegex(start, end) => Json.obj("start" -> start.toInt, "end" -> end.toInt)
        }
      ).parse("companies.csv", Schemas.basic.companySchema)(
          "features" -> resource("features.csv", "id"),
          "products" -> resource("products.csv", "id" <-> "id".splitKey)
        )(resourceSet)

      println("> " + result.left.get)
      result must beLeft
    }

    "attribute path join" in {

      val companyResource = QBResource("companies.csv", mkInputStream(Data.basic.companyData))
      val featureResource = QBResource("products.csv", mkInputStream(Data.basic.productData))
      val resourceSet = QBResourceSet(companyResource, featureResource)

      val companySchema = qbClass(
        "id" -> qbInteger,
        "company" -> Schemas.basic.companyCoreSchema,
        "products" -> qbClass(
          "options" -> qbList(Schemas.basic.productSchema)
        )
      )

      val result = CSVImporter().parse("companies.csv", companySchema)(
        "products.options" -> resource("products.csv", "id")
      )(resourceSet)

      result must beRight
      result.right.get(0) must beEqualTo(Json.obj(
        "id" -> 1.0,
        "company" -> Json.obj(
          "name" -> "Dude GmbH",
          "openHours" -> "14-18"
        ),
        "products" -> Json.obj(
          "options" -> Json.arr(
            Json.obj(
              "name" -> "Beer",
              "price" -> 5.0
            ),
            Json.obj(
              "name" -> "Pizza",
              "price" -> 8.0
            )
          )
        )
      )
      )
    }

    "attribute path join with validation " in {

      val companyResource = QBResource("companies.csv", mkInputStream(Data.basic.companyData))
      val featureResource = QBResource("products.csv", mkInputStream(Data.basic.productData))
      val resourceSet = QBResourceSet(companyResource, featureResource)


      val companySchema = qbClass(
        "id" -> qbInteger,
        "company" -> Schemas.basic.companyCoreSchema,
        "products" -> qbClass(
          "options" -> qbList(Schemas.basic.productSchema)
        )
      )

      val result = QBCSVValidator().parse("companies.csv", companySchema)(
        "products.options" -> resource("products.csv", "id")
      )(resourceSet)

      result must beRight
      result.right.get(0) must beEqualTo(Json.obj(
        "id" -> 1.0,
        "company" -> Json.obj(
          "name" -> "Dude GmbH",
          "openHours" -> "14-18"
        ),
        "products" -> Json.obj(
          "options" -> Json.arr(
            Json.obj(
              "name" -> "Beer",
              "price" -> 5.0
            ),
            Json.obj(
              "name" -> "Pizza",
              "price" -> 8.0
            )
          )
        )
      )
      )
    }

    "attribute path join with validation violation " in {

      val companyResource = QBResource("companies.csv", mkInputStream(Data.basic.companyData))
      val featureResource = QBResource("products.csv", mkInputStream(Data.basic.productData))
      val resourceSet = QBResourceSet(companyResource, featureResource)

      val productSchema = qbClass(
        "name" -> qbString,
        "price" -> qbNumber(range(10, 99))
      )

      val companySchema = qbClass(
        "id" -> qbInteger,
        "company" -> Schemas.basic.companyCoreSchema,
        "products" -> qbClass(
          "options" -> qbList(productSchema)
        )
      )

      val result = QBCSVValidator().parse("companies.csv", companySchema)(
        "products.options" -> resource("products.csv", "id")
      )(resourceSet)

      println("<" + result.left.get)
      result must beLeft
      // TODO: fine grained check for violation
    }

    "be able to split arrays when joining" in {

      val companyData = """id;company.name;company.openHours;products.colors
            1;Dude GmbH;14-18;brown,red
            2;Nerd Inc.;22-24;black""".stripMargin

      val companyResource = QBResource("companies.csv", mkInputStream(companyData))
      val featureResource = QBResource("products.csv", mkInputStream(Data.extended.productData))
      val resourceSet = QBResourceSet(companyResource, featureResource)

      val companySchema = qbClass(
        "id" -> qbInteger,
        "company" -> Schemas.basic.companyCoreSchema,
        "products" -> qbClass(
          "options" -> qbList(Schemas.basic.productSchema),
          "colors" -> qbList(qbString)
        )
      )

      val result = CSVImporter(
        "products.colors" --> { case cell: String => JsArray(cell.split(',').map(JsString).toList) }
      ).parse("companies.csv", companySchema)(
          "products.options" -> resource("products.csv", "id")
        )(resourceSet)

      result must beRight
      result.right.get(0) must beEqualTo(Json.obj(
        "id" -> 1.0,
        "company" -> Json.obj(
          "name" -> "Dude GmbH",
          "openHours" -> "14-18"
        ),
        "products" -> Json.obj(
          "colors" -> JsArray(Seq(JsString("brown"), JsString("red"))),
          "options" -> JsArray(Seq(
            Json.obj(
              "name" -> "Beer",
              "price" -> 5.0
            ),
            Json.obj(
              "name" -> "Pizza",
              "price" -> 8.0
            )
          ))
        )
      )
      )
    }

    "allow arrays with simple value in one column" in {
      val csv = "array\n\"a\nb\""
      val schema = qbClass(
        "array" -> qbList(qbString))

      val importer = CSVImporter("array" --> {
        case x: String => JsArray(x.split("\n").map(JsString))
      })

      val result = importer.parse(schema, QBResource("resource", mkInputStream(csv)))

      result.right.get must have size 1
      result.right.get(0) must beEqualTo(Json.obj(
        "array" -> List("a", "b")))
    }

    "allow arrays with multiple values in one column" in {
      val csv = "array\n\"1//2\n3//4\""

      val arrayItem = qbClass(
        "first" -> qbString,
        "second" -> qbString
      )

      val schema = qbClass(
        "array" -> qbList(arrayItem)
      )

      val importer = CSVImporter(
        "array" --> {
          case x: String =>
            val fieldsList = x.split("\n").toList.map(_.split("//").toList)
            JsArray(fieldsList.map(fields => jsonObj(arrayItem, fields)))
        })

      val result = importer.parse(schema, QBResource("resource", mkInputStream(csv)))

      result must beRight
      result.right.get(0) must beEqualTo(Json.obj(
        "array" -> List(Json.obj(
          "first" -> "1",
          "second" -> "2"), Json.obj(
          "first" -> "3",
          "second" -> "4"))))
    }
  }
}
