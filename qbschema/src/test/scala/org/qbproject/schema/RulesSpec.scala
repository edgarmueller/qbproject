package org.qbproject.schema

import org.specs2.runner.JUnitRunner
import org.specs2.mutable.Specification
import QBSchema._
import org.junit.runner.RunWith
import play.api.libs.json._

@RunWith(classOf[JUnitRunner])
class RulesSpec extends Specification {

  "String Rules" should {

    "nonEmpty" in {
      qbNonEmptyText.validate(JsString("a")).isSuccess must beTrue
      qbNonEmptyText.validate(JsString("")).isSuccess must beFalse
    }

    "minlength" in {
      val text = "0123456789"
      minLength(4).validate(JsString(text)).isSuccess must beTrue
      minLength(11).validate(JsString(text)).isSuccess must beFalse
    }

    "maxlength" in {
      val text = "0123456789"
      maxLength(11).validate(JsString(text)).isSuccess must beTrue
      maxLength(4).validate(JsString(text)).isSuccess must beFalse
    }

    "enum" in {
      qbEnum("eddy", "otto", "dude").validate(JsString("dude")).isSuccess must beTrue
      qbEnum("eddy", "otto", "dude").validate(JsString("honk")).isSuccess must beFalse
    }

    "email (pattern)" in {
      qbEmail.validate(JsString("otto@m-cube.de")).isSuccess must beTrue
      qbEmail.validate(JsString("dude@@dude")).isSuccess must beFalse
    }

  }

  "Number Rules" should {

    "validate against a min constraint" in {
      min(10).validate(JsNumber(10)).isSuccess must beTrue
      min(10).validate(JsNumber(5)).isSuccess must beFalse
    }

    "max" in {
      max(10).validate(JsNumber(5)).isSuccess must beTrue
      max(10).validate(JsNumber(11)).isSuccess must beFalse
    }

  }

  "Boolean Rules" should {

    "validate JsBoolean correctly" in {
      qbBoolean.validate(JsBoolean(true)).isSuccess must beTrue
      qbBoolean.validate(JsBoolean(false)).isSuccess must beTrue
    }

  }

  "Uniqueness Rules" should {

    "check return true if the list only contains distinct elements" in {
      qbList(qbNumber, unique).validate(JsArray(Seq(JsNumber(1), JsNumber(2), JsNumber(3)))).isSuccess must beTrue
    }

    "check return false if a list contains duplicates" in {
      qbList(qbNumber, unique).validate(JsArray(Seq(JsNumber(1), JsNumber(2), JsNumber(2), JsNumber(3)))).isSuccess must beFalse
    }
  }
 

}