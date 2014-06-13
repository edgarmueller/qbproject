package org.qbproject.api.csv

import java.io.{ByteArrayInputStream}
import play.api.libs.json._
import play.api.libs.json.JsArray
import play.api.libs.json.JsString
import org.specs2.mutable.Specification
import org.qbproject.csv.Path
import org.qbproject.api.schema.QBSchema._

object CSVAdapterTest extends Specification {

  "CSV Adapter" should {

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

    /**
     * Tests ---
     */

    "read a basic CSV file" in {
      val adapter = CSVAdapter()
      val bis = new ByteArrayInputStream(Data.basic.companyData.getBytes("UTF-8"))
      val companies = adapter.parse(Schemas.basic.companySchema keep ("id", "company"), QBResource("companies.csv", bis))
      bis.close
      val firstCompany = companies(0).get
      firstCompany must beEqualTo(Json.obj(
        "id" -> 1, "company" -> Json.obj(
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

      val jsResults = CSVAdapter(
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

      val result = jsResults.get

      result(0) must beEqualTo(Json.obj(
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

      result(1) must beEqualTo(Json.obj(
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

      val result = CSVAdapter(
        Path("range") -> {
          case rangeRegex(start, end) => Json.obj("start" -> start.toInt, "end" -> end.toInt)
        }
      ).parse("companies.csv", Schemas.basic.companySchema)(
          "features" -> resource("features.csv", "id"),
          "products" -> resource("products.csv", "id" <-> "id".splitKey)
        )(resourceSet)

      resourceSet.close

      result.get(0) must beEqualTo(
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

      result.get(1) must beEqualTo(
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

      val result = CSVAdapter(
        Path("range") -> {
          case rangeRegex(start, end) => Json.obj("start" -> start.toInt, "end" -> end.toInt)
        }
      ).parse("companies.csv", Schemas.basic.companySchema)(
          "features" -> resource("features.csv", "id"),
          "products" -> resource("products.csv", "id" <-> "id".splitKey)
        )(resourceSet)

      resourceSet.close

      println(JsError.toFlatJson(result.asInstanceOf[JsError]))

      result must beAnInstanceOf[JsError]
    }

    "report errors if CSV does not conform to schema" in {

      val companyData = """id;company.name;company.openHours
            1;Dude GmbH;14-18
            3;Dude GmbH;14-18
            4x;Dude GmbH;14-18
            5x;Dude GmbH;14-18
            2xxx;Nerd Inc.;12-24""".stripMargin

      val companyResource = QBResource("companies.csv", mkInputStream(companyData))
      val featureResource = QBResource("features.csv", mkInputStream(Data.basic.featureData))
      val resourceSet = QBResourceSet(companyResource, featureResource)

      val result = CSVAdapter().parse("companies.csv", Schemas.basic.companySchema -- ("products"))(
          "features" -> resource("features.csv", "id")
      )(resourceSet)

      resourceSet.close

      println(result)

      val bla = result.asInstanceOf[JsError].errors.map { pathWithError =>
        val messages = pathWithError._2.map(_.message).mkString("\n")
        messages
      }
//      println(bla)

      println(Json.prettyPrint(JsError.toFlatJson(result.asInstanceOf[JsError])))
      println(JsError.toFlatForm(result.asInstanceOf[JsError]))

      result must beAnInstanceOf[JsError]
    }

    "TODO: inner join(?)" in {

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

      val result = CSVAdapter().parse("companies.csv", companySchema)(
        "products.options" -> resource("products.csv", "id")
      )(resourceSet)

      result.get(0) must beEqualTo(Json.obj(
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

    "TODO: split arrays" in {


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

      val result = CSVAdapter(
        "products.colors" --> { case cell: String => JsArray(cell.split(',').toList.map(JsString)) }
      ).parse("companies.csv", companySchema)(
        "products.options" -> resource("products.csv", "id")
      )(resourceSet)

      result.get(0) must beEqualTo(Json.obj(
        "id" -> 1.0,
        "company" -> Json.obj(
          "name" -> "Dude GmbH",
          "openHours" -> "14-18"
        ),
        "products" -> Json.obj(
          "colors" -> Json.arr("brown", "red"),
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
  }
}
