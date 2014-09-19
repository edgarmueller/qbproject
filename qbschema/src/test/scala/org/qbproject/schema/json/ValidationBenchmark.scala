package org.qbproject.schema.json

import org.qbproject.schema.QBSchema._
import org.qbproject.schema._
import org.scalameter.api._
import play.api.data.validation.ValidationError
import play.api.libs.json.Json._
import play.api.libs.json._

import scala.util.Random
import scalaz.{Failure, Success}


object ValidationBenchmark extends PerformanceTest {

  val WIDTH = 1
  val DEPTH = 1

  /* configuration */
  override lazy val reporter = new LoggingReporter

  lazy val executor = LocalExecutor(
    new Executor.Warmer.Default,
    Aggregator.min,
    new Measurer.Default)
  lazy val persistor = Persistor.None


  val numberOfInstancesGen = Gen.range("numberOfInstances")(100, 500, 100)

  val instances = for {
    numberOfInstances <- numberOfInstancesGen
  } yield BenchmarkInstanceGenerator.generate(numberOfInstances)

  performance of "QBValidator" in {
    measure method "validate" in {
      using(instances) in { t =>
        validatorRun(t)
      }
    }
  }

  performance of "MyValidator" in {
    measure method "validate" in {
      using(instances) in { t =>
        t.map {
          obj =>
            new MyValidator(BenchmarkSchema.auto).validate1(obj)
        }
      }
    }
  }

  def validatorRun(objs: List[JsObject]) = {
    objs.map {
      instance =>
        QBValidator.validate(BenchmarkSchema.auto)(instance)
      //        match {
      //          case s: JsSuccess[_] => println("s")
      //          case e: JsError => println(e)
      //        }
    }
  }

  def generate(depth: Int, fieldsPerObject: Int): (QBClass, JsObject) = {
    val start = (qbClass(), obj())
    if (depth == 0) {
      start
    } else {
      (0 to fieldsPerObject)
        .foldLeft(start) {
        (t: (QBClass, JsObject), idx: Int) =>
          val name = "field" + idx

          val sub = idx % 5 match {
            case 0 => (qbBoolean, JsBoolean(true))
            case 1 => (qbNumber(), JsNumber(42))
            case 2 => (qbString, JsString("Some Text"))
            case 3 =>
//              val subsub = generate(depth - 1, fieldsPerObject)
              (qbList(qbClass()), JsArray((0 to fieldsPerObject).map({ x: Int => obj()})))
            case 4 => generate(depth - 1, fieldsPerObject)
          }

          (t._1 ++ qbClass(name -> sub._1), t._2 + (name -> sub._2))
      }
    }
  }

}

object BenchmarkInstanceGenerator {

  import play.api.libs.json.Json._

  val r = new Random(12345)

  def generate(): JsObject = {
    obj(
      "meta" -> obj(
        "name" -> generateString(10),
        "make" -> generateString(15),
        "year" -> generateNumberString(4),
        "price" -> r.nextInt()
      ),
      "extra" -> arr(
        obj(
          "name" -> generateString(10),
          "description" -> generateString(1000),
          "price" -> r.nextInt()
        )
      ),
      "tires" -> arr(
        obj(
          "diameter" -> r.nextInt(30),
          "width" -> r.nextInt(20),
          "color" -> "red",
          "material" -> generateString(10)
        )
      ),
      "technicalData" -> obj(
        "engine" -> obj(
          "capacity" -> r.nextInt(),
          "torque" -> r.nextInt(),
          "power" -> r.nextInt()
        ),
        "maxVelocity" -> r.nextInt(),
        "weight" -> r.nextInt()
      ),
      "interior" -> obj(
        "colorOne" -> generateString(10),
        "colorTwo" -> generateString(10),
        "colorThree" -> generateString(10)
      ),
      "exterior" -> obj(
        "colorOne" -> generateString(10),
        "colorTwo" -> generateString(10)
      ),
      "objects" -> createObject(ValidationBenchmark.DEPTH, ValidationBenchmark.WIDTH)
    )
  }

  def createObject(depth: Int, width: Int): JsObject = {
    if (depth <= 0) {
      obj(
        Range(0, width).map {
          idx =>
            idx.toString -> toJsFieldJsValueWrapper(generateString(10))
        }: _ *
      )
    } else {
      obj(
        Range(0, width).map {
          idx =>
            idx.toString -> toJsFieldJsValueWrapper(createObject(depth - 1, width))
        }: _ *
      )
    }
  }

  def generate(cnt: Int): List[JsObject] = {
    (0 until cnt).map {
      idx =>
        generate()
    }.toList
  }

  def generateString(cnt: Int): String = {
    r.nextString(cnt)
  }

  def generateNumberString(cnt: Int): String = {
    val builder = new StringBuilder()
    for (i <- 0 until cnt) {
      builder.append(r.nextInt(10))
    }
    builder.toString()
  }

}

object BenchmarkSchema {

  val tire = qbClass(
    "diameter" -> qbNumber,
    "width" -> qbNumber,
    "color" -> qbString,
    "material" -> qbString
  )

  val color = qbEnum("red", "blue", "green", "yellow", "magenta", "cyan")

  val auto = qbClass(
    "meta" -> qbClass(
      "name" -> qbString,
      "make" -> qbString,
      "year" -> qbString,
      "price" -> qbNumber
    ),
    "extra" -> qbList(qbClass(
      "name" -> qbString,
      "description" -> qbText,
      "price" -> qbNumber
    )),
    "tires" -> qbList(tire),
    "technicalData" -> qbClass(
      "engine" -> qbClass(
        "capacity" -> qbNumber,
        "torque" -> qbNumber,
        "power" -> qbNumber
      ),
      "maxVelocity" -> qbNumber,
      "weight" -> qbNumber

    ),
    "interior" -> qbClass(
      "colorOne" -> qbString,
      "colorTwo" -> qbString,
      "colorThree" -> qbString
    ),
    "exterior" -> qbClass(
      "colorOne" -> qbString,
      "colorTwo" -> qbString
    ),
    "objects" -> createObject(ValidationBenchmark.DEPTH, ValidationBenchmark.WIDTH)
  )

  def createObject(depth: Int, width: Int): QBClass = {
    if (depth <= 0) {
      qbClass(
        Range(0, width).map {
          idx =>
            idx.toString -> qbString
        }: _ *
      )
    } else {
      qbClass(
        Range(0, width).map {
          idx =>
            idx.toString -> createObject(depth - 1, width)
        }: _ *
      )
    }
  }

}

class MyValidator(val schema: QBClass) {

  def validate(value: JsValue, bType: QBType): JsResult[JsValue] = {
    (value, bType) match {
      case (v: JsNumber, t: QBNumber) =>
        t.validate(v) match {
          case Success(s) => JsSuccess(v)
          case Failure(e) => JsError("Expected: " + t + " Actual: " + v)
        }
      case (v: JsNumber, t: QBInteger) =>
        t.validate(v) match {
          case Success(s) => JsSuccess(v)
          case Failure(e) => JsError("Expected: " + t + " Actual: " + v)
        }
      case (v: JsBoolean, t: QBBoolean) =>
        t.validate(v) match {
          case Success(s) => JsSuccess(v)
          case Failure(e) => JsError("Expected: " + t + " Actual: " + v)
        }
      case (v: JsString, t: QBString) =>
        t.validate(v) match {
          case Success(s) => JsSuccess(v)
          case Failure(e) => JsError("Expected: " + t + " Actual: " + v)
        }
      case (v: JsArray, t: QBArray) =>
        v.value.map { item => validate(item, t.items)}.foldLeft[JsResult[JsValue]](JsSuccess(v)) {
          (res: JsResult[_], itemRes: JsResult[_]) => (res, itemRes) match {
            case (total: JsError, e: JsError) => JsError.merge(total, e)
            case (_, e: JsError) => e
            case (JsSuccess(s: JsValue, _), _) => JsSuccess(s)
          }
        }
      case (v: JsObject, t: QBClass) =>
        t.attributes.map {
          attr =>
            val name = attr.name
            val attrValue = v \ name
            validate(attrValue, attr.qbType)
        }.foldLeft[JsResult[JsValue]](JsSuccess(v)) {
          (res: JsResult[_], itemRes: JsResult[_]) => (res, itemRes) match {
            case (total: JsError, e: JsError) => JsError.merge(total, e)
            case (_, e: JsError) => e
            case (JsSuccess(s: JsValue, _), _) => JsSuccess(s)
          }
        }
      case _ => JsError("Expected: " + bType + " Actual: " + value)
    }
  }

  def validate1(json: JsValue): JsResult[JsValue] = validate(json, schema)

}
