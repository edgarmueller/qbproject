package org.qbproject.api.csv

import org.specs2.mutable.Specification
import java.io.{ByteArrayInputStream, File, FileInputStream}
import org.qbproject.api.schema.QBSchema._
import play.api.libs.json._
import scala.util.{Success, Try}
import org.qbproject.api.schema.QBClass
import org.qbproject.csv._
import org.qbproject.csv.ResourceReference
import play.api.libs.json.JsArray
import org.qbproject.csv.SplitJoinKey
import play.api.libs.json.JsString
import org.qbproject.csv.Path
import org.qbproject.csv.JoinKey

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
            2;Nerd Inc.;22-24;e1,e2,e3""".stripMargin

        val employeeData = """eId;name;tags[0];tags[1]
            e1;Eddy;dev;
            e2;Otto;dev;ginger
            e3;Max;senior architect;""".stripMargin

        override val productData = """id;name;price;range
            1;Beer;5;2-3
            1;Pizza;8;4-5
            2;Hacking;100;6-99""".stripMargin
      }
    }

    object Schemas {

      trait _basic {
        val companyCoreSchema = qbClass(
          "name" -> qbString,
          "openHours" -> qbString) // TODO: provide a range type
        // "employees" -> qbList(qbString))

        val featureSchema = qbClass(
          "strength" -> qbInteger,
          "speed" -> qbInteger
        )

        val productSchema = qbClass(
          "name" -> qbString,
          "price" -> qbNumber
        )

        def companySchema = qbClass(
          "id" -> qbNumber,
          "company" -> companyCoreSchema,
          "features" -> featureSchema,
          "products" -> productSchema
        )
      }

      val basic = new _basic{}

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
          "id" -> qbNumber,
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
      val companies = adapter.parse(Schemas.basic.companySchema keep ("id", "company"), bis)
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

      val result = CSVAdapter(
        Path("range") -> {
          case rangeRegex(start, end) => Json.obj("start" -> start.toInt, "end" -> end.toInt)
        },
        MappedPath("employees", "company.employees") -> {
          case employees: String => JsArray(employees.split(",").toList.map(JsString))
        }
      ).parse("companies.csv", Schemas.extended.companySchema)(
          "features" -> resource("features.csv", "id"),
          "products" -> resource("products.csv", "id"),
          "employees" -> resource("employees.csv", "employees".splitKey <-> "eId")
      )(resourceSet)

      resourceSet.close

      result.get(0) must beEqualTo(Json.obj(
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

      result.get(1) must beEqualTo(Json.obj(
        "id" -> 2,
        "company" -> Json.obj(
          "name" -> "Nerd Inc.",
          "openHours" -> "22-24"
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
          Json.obj(
            "strength" -> 2.0,
            "speed" -> 4.0
          )
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
  }
}
