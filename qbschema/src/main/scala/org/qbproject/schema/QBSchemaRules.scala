package org.qbproject.schema

import java.util.regex.Pattern

import scala.collection.immutable.HashSet
import scala.util.Try
import scalaz.{Success, Failure, Validation}
import org.joda.time.DateTime
import play.api.libs.json._
import play.api.data.validation.ValidationError

//----------------------------------------------------------
// 	Rule definitions
//----------------------------------------------------------
/**
 * Definition of a basic validation rule.
 *
 * @tparam TO_VALIDATE
 *                     the type of the argument that needs to be validated
 */
trait ValidationRule[TO_VALIDATE] {

  type ERRORS = List[ValidationError]
  
  /**
   * Validates the given object and returns the validation status
   *
   * @param a
             the object to be validated
   * @return
   *         a validation result that may succeed or fail
   */
  def validate(a: TO_VALIDATE): Validation[ERRORS, TO_VALIDATE]
}

/**
 * Definition of a validation rule that consists of multiple different validation rules.
 *
 * @tparam A
 *           the type of the argument that needs to be validated
 */
trait CompositeRule[A <: JsValue] extends ValidationRule[A] {

  /**
   * Returns all rules that make up the composite rule.
   *
   * @return a set of validation rules
   */
  def rules: Set[ValidationRule[A]]

  /**
   * Validates the given object and returns the validation status
   *
   * @param a
             the object to be validated
              * @return
   * a validation result that may succeed or fail
   */
  override def validate(a: A): Validation[ERRORS, A] = {
    rules.foldLeft[Validation[ERRORS, A]](Success(a)) {
      (acc, rule) => (acc, rule.validate(a)) match {
        case (Success(s1), Success(s2)) => Success(s1)
        case (Success(s1), Failure(f2)) => Failure(f2)
        case (Failure(f1), Success(s2)) => Failure(f1)
        case (Failure(e1: ERRORS), Failure(e2: ERRORS)) => Failure(e1 ::: e2)
      }
    }
  }
}

//----------------------------------------------------------
// 	Number Rules
//----------------------------------------------------------

/**
 * Rule that checks whether a given number is a multiple of another number.
 *
 * @param multiple
 *           a factor of the number to be validated, if this rule should validate successfully
 */
case class MultipleOfRule(multiple: Double) extends ValidationRule[JsNumber] {
  override def validate(number: JsNumber): Validation[ERRORS, JsNumber] = {
    if(number.value.toDouble % multiple == 0) {
      Success(number)
    } else {
      Failure(List(ValidationError("qb.number.multipleof.violated")))
    }
  }
}

/**
 * Rule that checks whether a given number is greater than or equal to a given minimum.
 *
 * @param min
 *            the minimum which needs to be less than or equal to the number that is validated
 * @param isExclusive
 *            if true, the check also succeeds if the number to be checked is equal to the minimum
 */
case class MinRule(min: Double, isExclusive: Boolean) extends ValidationRule[JsNumber] {
  override def validate(number: JsNumber): Validation[ERRORS, JsNumber] = {
    if(isValid(number)) {
      Success(number)
    } else {
      Failure(List(ValidationError("qb.number.min.rule.violated")))
    }
  }
  def isValid(n: JsNumber) = {
    if (isExclusive) {
      n.value.toDouble > min
    } else {
      n.value.toDouble >= min
    }
  }
}

/**
 * Rule that checks whether a given number is less than or equal to a given minimum.
 *
 * @param max
 *            the maximum which needs to be greater than or equal to the number that is validated
 * @param isExclusive
 *            if true, the check also succeeds if the number to be checked is equal to the maximum
 */
case class MaxRule(max: Double, isExclusive: Boolean) extends ValidationRule[JsNumber] {
  override def validate(number: JsNumber): Validation[ERRORS, JsNumber] = {
    if(isValid(number)) {
      Success(number)
    } else {
      Failure(List(ValidationError("qb.number.max.rule.violated")))
    }
  }
  def isValid(n: JsNumber) = {
    if (isExclusive) {
      n.value.toDouble < max
    } else {
      n.value.toDouble <= max
    }
  }
}

/**
 * Utility wrapper class that is used in the DSL.
 *
 * @param rule
 *             the wrapped number rule
 */
case class DoubleRuleWrapper(rule: ValidationRule[JsNumber])

//----------------------------------------------------------
// 	String Rules
//----------------------------------------------------------

/**
 * Rule that checks whether a string has a minimum length.
 *
 * @param minLength
 *           the minimum length of the string
 */
case class MinLengthRule(minLength: Int) extends ValidationRule[JsString] {
  override def validate(str: JsString): Validation[ERRORS, JsString] = {
    if(str.value.length >= minLength) {
      Success(str)
    } else {
      Failure(List(ValidationError("qb.string.min.length.violated")))
    }
  }
}

/**
 * Rule that checks whether a string does not exceeds a maximum length.
 *
 * @param maxLength
 *           the maximum length of the string that must not be exceeded
 */
case class MaxLengthRule(maxLength: Int) extends ValidationRule[JsString] {
  override def validate(str: JsString): Validation[ERRORS, JsString] = {
    if(str.value.length < maxLength) {
      Success(str)
    } else {
      Failure(List(ValidationError("qb.string.max.length.violated")))
    }
  }
}

/**
 * Rule that checks whether a string matches regular expression.
 *
 * @param pattern
 *           the regular expression to be matched
 */
case class RegexRule(pattern: Pattern) extends ValidationRule[JsString] {
  override def validate(str: JsString): Validation[ERRORS, JsString] = {
    if(pattern.matcher(str.value).matches()) {
      Success(str)
    } else {
      Failure(List(ValidationError(errorMessage(str.value))))
    }
  }

  def errorMessage(input: String) = s"'$input' doesn't comply with RegEx '${pattern.pattern()}'"
}

object RegexRule {
  def apply(regex: String) = new RegexRule(Pattern.compile(regex))
}

/**
 * Rule that checks whether a string matches is contained in a set of predefined strings.
 *
 * @param enum
 *           the valid strings
 */
case class EnumRule(enum: List[String]) extends ValidationRule[JsString] {
  val values = HashSet(enum:_ *)
  override def validate(str: JsString): Validation[ERRORS, JsString] = {
    if(values.contains(str.value)) {
      Success(str)
    } else {
      Failure(List(ValidationError("'" + str.value + "' is not one of " + enum.mkString(", "))))
    }
  }
}

//----------------------------------------------------------
// 	Array Rules
//----------------------------------------------------------

/**
 * Rule that checks whether all items of an array are unique.
 */
case class UniquenessRule() extends ValidationRule[JsArray] {
  override def validate(arr: JsArray): Validation[ERRORS, JsArray] = {
    if(arr.value.distinct.size == arr.value.size) {
      Success(arr)
    } else {
      Failure(List(ValidationError("qb.arr.uniqueness.violated")))
    }
  }
}

//----------------------------------------------------------
// 	Object Rules
//----------------------------------------------------------

/**
 * Rule that checks whether a given object has a minimum number of properties.
 * 
 * @param minProperties
 *            the minimum number of properties
 */
case class MinPropertiesRule(minProperties: Int) extends ValidationRule[JsObject] {
  override def validate(obj: JsObject): Validation[ERRORS, JsObject] = {
    if(obj.fieldSet.size >= minProperties) {
      Success(obj)
    } else {
      Failure(List(ValidationError("qb.min.props.violated")))
    }
  }
}

/**
 * Rule that checks whether the number of properties of a given object does not exceed the given maximum number
 * of properties.
 *
 * @param maxProperties
 *            the minimum number of properties
 */
case class MaxPropertiesRule(maxProperties: Int) extends ValidationRule[JsObject] {
  override def validate(obj: JsObject): Validation[ERRORS, JsObject] = {
    if(obj.fieldSet.size <= maxProperties) {
      Success(obj)
    } else {
      Failure(List(ValidationError("qb.max.props.violated")))
    }
  }
}

case class KeyValueRule[A](key: String, value: String) extends ValidationRule[A] {
  override def validate(a: A): Validation[ERRORS, A] = Success(a)
}

//----------------------------------------------------------
// 	Format Rules
//----------------------------------------------------------
/**
 * Format rule definition.
 *
 * @tparam A
 *           the type to be validated
 */
trait FormatRule[A] extends ValidationRule[A] {
  def format: String
}

/**
 * Rule that checks whether a string matches the format of a datetime, that is, whether it can be parsed the
 * Joda DateTime class.
 */
object DateTimeRule extends FormatRule[JsString] {
  val format = "date-time"

  override def validate(str: JsString): Validation[ERRORS, JsString] = {
    if(Try(new DateTime(str.value)).isSuccess) {
      Success(str)
    } else {
      Failure(List(ValidationError("qb.format.date-time")))
    }
  }
}

/**
 * Rule that checks whether a string matches the format of a POSIX time, that is, whether it number
 * is whole at positive.
 */
object PosixTimeRule extends FormatRule[JsNumber] {
  val format = "posix-time"

  override def validate(d: JsNumber): Validation[ERRORS, JsNumber] = {
    if(d.value.toDouble.isWhole && d.value.toDouble > 0) {
      Success(d)
    } else {
      Failure(List(ValidationError("qb.format.posix-time")))
    }
  }
}
