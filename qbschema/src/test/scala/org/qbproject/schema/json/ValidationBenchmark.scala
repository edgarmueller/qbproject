package org.qbproject.schema.json

import org.qbproject.schema.QBSchema._
import org.qbproject.schema.{QBClass, QBValidator}
import org.scalameter.api._
import play.api.libs.json.Json._
import play.api.libs.json._


object ValidationBenchmark extends PerformanceTest {

  /* configuration */
  override lazy val reporter = new LoggingReporter

  lazy val executor = LocalExecutor(
    new Executor.Warmer.Default,
    Aggregator.min,
    new Measurer.Default)
  lazy val persistor = Persistor.None


  val depths = Gen.range("depth")(1, 5, 1)
  val fieldsPerObjects = Gen.range("fieldsPerObject")(0, 30, 5)

  val schemasAndInstances = for {
    depth <- depths
    fieldsPerObject <- fieldsPerObjects
  } yield generate(depth, fieldsPerObject)

  performance of "QBValidator" in {
    measure method "validate" in {
      using(schemasAndInstances) in { t =>
        val result = QBValidator.validate(t._1)(t._2)
        if (result.isInstanceOf[JsError]) {
          println(result)
        }
        result
      }
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
