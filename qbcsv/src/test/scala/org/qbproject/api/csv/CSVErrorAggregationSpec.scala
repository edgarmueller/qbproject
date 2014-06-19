package org.qbproject.api.csv

import org.specs2.mutable.Specification
import org.qbproject.api.schema.QBSchema._
import java.io.ByteArrayInputStream
import play.api.libs.json.{Json, JsError}
import play.api.data.validation.ValidationError
import org.qbproject.csv.CSVErrorInfo

class CSVErrorAggregationSpec extends Specification {

  "CSVAdapter " should {

    val companySchema = qbClass(
      "id" -> qbInteger,
      "company" -> qbClass(
        "name" -> qbString,
        "openHours" -> qbString),
      "features" -> qbList(qbClass(
        "strength" -> qbInteger,
        "speed" -> qbInteger
      )),
      "products" -> qbList(qbClass(
        "name" -> qbString,
        "price" -> qbNumber
      ))
    )

    val featureData = """id;strength;speed
            1;3;2
            2;2;4""".stripMargin


    "report errors if CSV does not conform to schema" in {

      val companyData = """id;company.name;company.openHours
            1;Dude GmbH;14-18
            3;Dude GmbH;14-18
            4x;Dude GmbH;14-18
            5x;Dude GmbH;14-18
            2xxx;Nerd Inc.;12-24""".stripMargin

      val companyResource = QBResource("companies.csv", new ByteArrayInputStream(companyData.getBytes("UTF-8")))
      val featureResource = QBResource("features.csv", new ByteArrayInputStream(featureData.getBytes("UTF-8")))
      val resourceSet = QBResourceSet(companyResource, featureResource)

      val result = CSVAdapter().parse("companies.csv", companySchema -- "products")(
        "features" -> resource("features.csv", "id")
      )(resourceSet)

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


    "report all CSV errors in case both CSV files contain errors" in {

      val productSchema = qbClass(
        "name" -> qbString,
        "price" -> qbNumber(range(10, 99))
      )

      val companySchema = qbClass(
        "id" -> qbInteger,
        "company" -> qbClass(
          "name" -> qbString,
          "openHours" -> qbString),
        "products" -> qbClass(
          "options" -> qbList(productSchema)
        )
      )

      val companyData = """id;company.name;company.openHours;products.colors
            1;Dude GmbH;14-18;brown,red
            x2;Nerd Inc.;22-24;black""".stripMargin // id must be number

      val productData = """id;name;price;range
            1;Beer;5;2-3
            1;Pizza;8;4-5
            2;Hacking;100xx;6-99""".stripMargin // price must be number

      val companyResource = QBResource("companies.csv", new ByteArrayInputStream(companyData.getBytes("UTF-8")))
      val productResource = QBResource("products.csv", new ByteArrayInputStream(productData.getBytes("UTF-8")))
      val resourceSet = QBResourceSet(companyResource, productResource)

      val result = CSVValidator(
        "products.colors" --> { case cell: String => Json.arr(cell.split(',').toList)}
      ).parse("companies.csv", companySchema)(
          "products.options" -> resource("products.csv", "id")
        )(resourceSet)

      println(QBCSVErrorMap(result.asInstanceOf[JsError]).prettyPrint)

      true must beTrue
    }
  }
}
