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
      qbNonEmptyText.isValid(JsString("a")) must beTrue
      qbNonEmptyText.isValid(JsString("")) must beFalse
    }

    "minlength" in {
      val text = "0123456789"
      minLength(4).isValid(JsString(text)) must beTrue
      minLength(11).isValid(JsString(text)) must beFalse
    }

    "maxlength" in {
      val text = "0123456789"
      maxLength(11).isValid(JsString(text)) must beTrue
      maxLength(4).isValid(JsString(text)) must beFalse
    }

    "enum" in {
      qbEnum("eddy", "otto", "dude").isValid(JsString("dude")) must beTrue
      qbEnum("eddy", "otto", "dude").isValid(JsString("honk")) must beFalse
    }

    "email (pattern)" in {
      qbEmail.isValid(JsString("otto@m-cube.de")) must beTrue
      qbEmail.isValid(JsString("dude@@dude")) must beFalse
    }

  }

  "Number Rules" should {

    "validate against a min constraint" in {
      min(10).isValid(JsNumber(10)) must beTrue
      min(10).isValid(JsNumber(5)) must beFalse
    }

    "max" in {
      max(10).isValid(JsNumber(5)) must beTrue
      max(10).isValid(JsNumber(11)) must beFalse
    }

  }

  "Boolean Rules" should {

    "validate JsBoolean correctly" in {
      qbBoolean.isValid(JsBoolean(true)) must beTrue
      qbBoolean.isValid(JsBoolean(false)) must beTrue
    }

  }

  "Uniqueness Rules" should {

    "check return true if the list only contains distinct elements" in {
      qbList(qbNumber, unique).isValid(JsArray(Seq(JsNumber(1), JsNumber(2), JsNumber(3)))) must beTrue
    }

    "check return false if a list contains duplicates" in {
      qbList(qbNumber, unique).isValid(JsArray(Seq(JsNumber(1), JsNumber(2), JsNumber(2), JsNumber(3)))) must beFalse
    }
  }
 

}