package org.qbproject.csv

import java.io.ByteArrayInputStream

import org.qbproject.schema._
import org.qbproject.schema.QBSchema._
import org.qbproject.csv.internal.CSVImporter
import org.specs2.mutable.Specification
import play.api.libs.json.{JsArray, JsString, _}
import play.api.libs.json.Json._

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

    "perform parsing" in {
      val companies = CSVImporter(Schemas.basic.companySchema keep ("id", "company")).parse(
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

    "perform One To One joins" in {

      val root =
        """id
          |1
          |2""".stripMargin

      val companyData = """id;name;openHours
            1;Dude GmbH;14-18
            2;Nerd Inc.;22-24""".stripMargin


      val rootResource = QBResource("root.csv", mkInputStream(root))
      val companyResource = QBResource("companies.csv", mkInputStream(companyData))
      val resourceSet = QBResourceSet(rootResource, companyResource)

      val companies = CSVImporter(Schemas.basic.companySchema.keep("id", "company")).parse(
        "root.csv"
      )(
          "company" -> resource("companies.csv", "id")
        )(resourceSet)

      companies must beRight

      val firstCompany = companies.right.get(0)
      firstCompany must beEqualTo(Json.obj(
        "id" -> 1,
        "company" -> Json.obj(
          "name" -> "Dude GmbH",
          "openHours" -> "14-18"
        )
      ))
    }

    "allow many to one joins with mapped multivalued property" in {
      val rangeRegex = "([0-9]+)\\s*-\\s*([0-9]+)".r

      val companyResource = QBResource("companies.csv", mkInputStream(Data.extended.companyData))
      val featureResource  = QBResource("features.csv", mkInputStream(Data.basic.featureData))
      val productsResource  = QBResource("products.csv", mkInputStream(Data.extended.productData))
      val employeeResource  = QBResource("employees.csv", mkInputStream(Data.extended.employeeData))
      val resourceSet = QBResourceSet(companyResource, featureResource, productsResource, employeeResource)

      val jsResults = CSVImporter(Schemas.extended.companySchema,
        "range"--> {
          case rangeRegex(start, end) => JsSuccess(Json.obj("start" -> start.toInt, "end" -> end.toInt))
          case _ => JsError("Wrong format")
        },
        "employees".maps("company.employees") --> {
          case employees: String => JsSuccess(JsArray(employees.split(",").toList.map(JsString)))
        }).parse("companies.csv")(
          "features" -> resource("features.csv", "id"),
          "products" -> resource("products.csv", "id"),
          "employees" -> resource("employees.csv", "employees".splitKey <-> "eId")
        )(resourceSet)

      jsResults must beRight
      jsResults.right.get(0) must beEqualTo(Json.obj(
        "id" -> 1.0,
        "company" -> Json.obj(
          "name" -> "Dude GmbH",
          "openHours" -> "14-18"
        ),
        "employees" -> Json.arr(
          Json.obj(
            "name" -> "Eddy",
            "tags" -> Json.arr("dev")
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
            "tags" -> Json.arr("dev")
          ),
          Json.obj(
            "name" -> "Otto",
            "tags" -> Json.arr("dev", "ginger")
          ),
          Json.obj(
            "name" -> "Max",
            "tags" -> Json.arr("senior architect")
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

      val result = CSVImporter( Schemas.basic.companySchema,
        "range" --> {
          case rangeRegex(start, end) => Json.obj("start" -> start.toInt, "end" -> end.toInt)
        }
      ).parse("companies.csv")(
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

      val result = CSVImporter(Schemas.basic.companySchema,
        "range" --> {
          case rangeRegex(start, end) => Json.obj("start" -> start.toInt, "end" -> end.toInt)
        }
      ).parse("companies.csv")(
          "features" -> resource("features.csv", "id"),
          "products" -> resource("products.csv", "id" <-> "id".splitKey)
        )(resourceSet)

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

      val result = CSVImporter(companySchema).parse("companies.csv")(
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

      val result = QBCSVValidator(companySchema).parse("companies.csv")(
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

      val result = QBCSVValidator(companySchema).parse("companies.csv")(
        "products.options" -> resource("products.csv", "id")
      )(resourceSet)

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

      val result = CSVImporter(companySchema,
        "products.colors" --> { case cell: String => JsArray(cell.split(',').map(JsString).toList) }
      ).parse("companies.csv")(
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

      val importer = CSVImporter(schema, "array" --> {
        case arr: String => JsArray(arr.split("\n").map(JsString))
      })

      val result = importer.parse(QBResource("resource", mkInputStream(csv)))

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

      val importer = CSVImporter(schema,
        "array" --> {
          case s: String =>
            val fieldsList = s.split("\n").toList.map(_.split("//").toList)
            JsArray(fieldsList.map(fields => jsonObj(arrayItem, fields)))
        })

      val result = importer.parse(QBResource("resource", mkInputStream(csv)))

      result must beRight
      result.right.get(0) must beEqualTo(Json.obj(
        "array" -> List(Json.obj(
          "first" -> "1",
          "second" -> "2"), Json.obj(
          "first" -> "3",
          "second" -> "4"))))
    }

    "allow objects in arrays to span multiple columns" in {

      val csv =
        """persons[0].name; persons[0].age; persons[1].name; persons[1].age
          |foo; 1234;    ;
          |Edd; 1337; Odd; 31337""".stripMargin

      val arrayItem = qbClass(
        "name" -> qbString,
        "age" -> qbNumber
      )

      val schema = qbClass(
        "persons" -> qbList(arrayItem)
      )

      val importer = CSVImporter(schema)

      val result = importer.parse(QBResource("resource", mkInputStream(csv)))

      result must beRight
      result.right.get(0) must beEqualTo(obj(
        "persons" -> arr(
          obj(
            "name" -> "foo",
            "age" -> 1234
          )
        )
      ))
      result.right.get(1) must beEqualTo(obj(
        "persons" -> arr(
          obj(
            "name" -> "Edd",
            "age" -> 1337
          ),
          obj(
            "name" -> "Odd",
            "age" -> 31337
          )
        )
      ))
    }

    "allow path constructors to fail" in {
      val csvData = """name;age
                      |Max;35
                      |;26
                      |;31""".stripMargin

      val personSchema = qbClass(
        "name" -> qbString,
        "age"  -> qbInteger
      )

      val csvImporter = CSVImporter(personSchema,
        "name" --> {
          case name: String if name.isEmpty => JsError("Person name must not be empty")
          case name: String => JsSuccess(JsString(name.toUpperCase))
        }
      )

      val result: Either[QBCSVErrorMap, List[JsValue]] = csvImporter.parse(QBResource("person-resource", mkInputStream(csvData)))

      result must beLeft
      // TODO: too many gets
      result.left.get.errors.get("person-resource").get must have size 2
      result.left.get.errors.get("person-resource").get.collect { case err@QBCSVDataError(msg, _, _, _) if
        msg == "Person name must not be empty" => err } must have size 2
    }

    // TODO: rename description

    "allow syntax sugar for path builders" in {
      val csvData = """name;age
                      |Max;35
                      |alex;26
                      |stefan;31""".stripMargin

      val personSchema = qbClass(
        "name" -> qbString,
        "age"  -> qbInteger
      )

      val csvImporter = CSVImporter(personSchema,
        "name" --> { case name: String if !name.isEmpty => JsString(name.toUpperCase) }
      )

      val result: Either[QBCSVErrorMap, List[JsValue]] = csvImporter.parse(QBResource("person-resource", mkInputStream(csvData)))
      result must beRight
      result.right.get(0) must beEqualTo (Json.obj("name" -> "MAX", "age" -> JsNumber(35)))
    }

    "allow syntax sugar-built path builders to fail" in {
      val csvData = """name;age
                      |Max;35
                      |alex;26
                      |stefan;31x""".stripMargin

      val personSchema = qbClass(
        "name" -> qbString,
        "age"  -> qbInteger
      )

      val csvImporter = CSVImporter(personSchema,
        "age" --> { case age: String => JsNumber(Integer.parseInt(age)) }
      )

      val result: Either[QBCSVErrorMap, List[JsValue]] = csvImporter.parse(QBResource("person-resource", mkInputStream(csvData)))
      result must beLeft
      result.left.get.errors.get("person-resource").get must have size 1
    }


    "allow path constructors to bind to arrays" in {

      val csv =
        """persons[0].name; persons[0].age; persons[1].name; persons[1].age
          |foo; 12|34;    ;
          |Edd; 13|37; Odd; 31|337""".stripMargin

      val arrayItem = qbClass(
        "name" -> qbString,
        "age" -> qbList(qbString)
      )

      val schema = qbClass(
        "persons" -> qbList(arrayItem)
      )

      val importer = CSVImporter(schema,
        "persons[].age" --> {
          case s: String =>
            s.split("\\|").map(JsString).foldLeft(JsArray()){
              (array, number) =>
                array.append(number)
            }
        })

      val result = importer.parse(QBResource("resource", mkInputStream(csv)))

      result must beRight
      result.right.get(0) must beEqualTo(obj(
        "persons" -> arr(
          obj(
            "name" -> "foo",
            "age" -> arr("12", "34")
          )
        )
      ))
      result.right.get(1) must beEqualTo(obj(
        "persons" -> arr(
          obj(
            "name" -> "Edd",
            "age" -> arr("13", "37")
          ),
          obj(
            "name" -> "Odd",
            "age" -> arr("31", "337")
          )
        )
      ))
    }

    "must ignore optional fields if they are not in the CSV" in {
      val csv =
        """id;name
          |1;foo
          |2;bar""".stripMargin

      val schema = qbClass(
        "id" -> qbString,
        "name" -> qbString,
        "tags" -> optional(qbList(qbString))
      )

      val result = CSVImporter(schema).parse(QBResource("resource", mkInputStream(csv)))

      result must beRight
      result.right.get(0) must beEqualTo(obj(
        "id" -> "1",
        "name" -> "foo"
      ))
      result.right.get(1) must beEqualTo(obj(
        "id" -> "2",
        "name" -> "bar"
      ))
    }

    "must ignore optional fields if they are not in the CSV and a path constructor is present" in {
      val csv =
        """id
          |1
          |2""".stripMargin

      val schema = qbClass(
        "id" -> qbString,
        "name" -> optional(qbString),
        "tags" -> optional(qbList(qbString))
      )

      val result = CSVImporter(schema,
        "name" --> {
          case name: String => JsString(name.toUpperCase)
        }
      ).parse(QBResource("resource", mkInputStream(csv)))

      result must beRight
      result.right.get(0) must beEqualTo(obj(
        "id" -> "1"
      ))
      result.right.get(1) must beEqualTo(obj(
        "id" -> "2"
      ))
    }

    "must set fallback values of optional fields if they are not in the CSV" in {
      val csv =
        """id;name
          |1;foo
          |2;bar""".stripMargin

      val schema = qbClass(
        "id" -> qbString,
        "name" -> qbString,
        "tags" -> optional(qbList(qbString), JsArray())
      )

      val result = CSVImporter(schema).parse(QBResource("resource", mkInputStream(csv)))

      result must beRight
      result.right.get(0) must beEqualTo(obj(
        "id" -> "1",
        "name" -> "foo",
        "tags" -> arr()
      ))
      result.right.get(1) must beEqualTo(obj(
        "id" -> "2",
        "name" -> "bar",
        "tags" -> arr()
      ))
    }

  }
}
