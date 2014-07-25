package org.qbproject.schema

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

  /**
   * Checks whether the given object is valid.
   *
   * @param a
   *          the object to be validated
   * @return true, if the object is valid, false otherwise
   */
  def isValid(a: TO_VALIDATE): Boolean

  /**
   * The error message in case validation fails.
   *
   * @return the error message
   */
  def errorMessage: String

  /**
   * Validates the given object and returns the validation status
   *
   * @param a
             the object to be validated
   * @return
   *         a validation result that may succeed or fail
   */
  def validate(a: TO_VALIDATE): Validation[ValidationError, TO_VALIDATE] = {
    if (isValid(a)) {
      Success(a)
    } else {
      Failure(ValidationError(errorMessage))
    }
  }
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
   * Checks whether the given object is valid by checking if all rules are valid.
   *
   * @param a
   *          the object to be validated
   * @return true, if the object is valid, false otherwise
   */
  override def isValid(a: A) = rules.forall(_.isValid(a))

  /**
   * @inheritdoc
   *
   * @return the error message
   */
  override def errorMessage =
    rules.foldLeft("")((msg,rule) => { /*println(msg + "/" + rule.errorMessage);*/ msg + rule.errorMessage })
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
  def isValid(number: JsNumber) = number.value.toDouble % multiple == 0
  val errorMessage = "qb.number.multipleof.violated"
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
  def isValid(n: JsNumber) = {
    if (isExclusive) {
      n.value.toDouble > min
    } else {
      n.value.toDouble >= min
    }
  }
  val errorMessage = "qb.number.min.rule.violated"
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
  def isValid(n: JsNumber) = {
    if (isExclusive) {
      n.value.toDouble < max
    } else {
      n.value.toDouble <= max
    }
  }
  val errorMessage = "qb.number.max.rule.violated"
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
  def isValid(str: JsString) = str.value.length >= minLength
  val errorMessage = "qb.string.min.length.violated"
}

/**
 * Rule that checks whether a string does not exceeds a maximum length.
 *
 * @param maxLength
 *           the maximum length of the string that must not be exceeded
 */
case class MaxLengthRule(maxLength: Int) extends ValidationRule[JsString] {
  def isValid(str: JsString) = str.value.length < maxLength
  val errorMessage = "qb.string.max.length.violated"
}

/**
 * Rule that checks whether a string matches regular expression.
 *
 * @param regex
 *           the regular expression to be matched
 */
case class RegexRule(regex: String) extends ValidationRule[JsString] {
  def isValid(str: JsString) = str.value.matches(regex)
  val errorMessage = "qb.string.regex.violated"
}

/**
 * Rule that checks whether a string matches is contained in a set of predefined strings.
 *
 * @param enum
 *           the valid strings
 */
case class EnumRule(enum: List[String]) extends ValidationRule[JsString] {
  def isValid(str: JsString) = enum.contains(str.value)
  val errorMessage = "qb.string.enum.violated"
}

//----------------------------------------------------------
// 	Array Rules
//----------------------------------------------------------

/**
 * Rule that checks whether all items of an array are unique.
 */
case class UniquenessRule() extends ValidationRule[JsArray] {
  def isValid(arr: JsArray) = arr.value.distinct.size == arr.value.size
  val errorMessage = "qb.arr.uniqueness.violated"
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
  def isValid(obj: JsObject) = obj.fieldSet.size >= minProperties
  val errorMessage = "qb.min.props.violated"
}

/**
 * Rule that checks whether the number of properties of a given object does not exceed the given maximum number
 * of properties.
 *
 * @param maxProperties
 *            the minimum number of properties
 */
case class MaxPropertiesRule(maxProperties: Int) extends ValidationRule[JsObject] {
  def isValid(obj: JsObject) = obj.fieldSet.size <= maxProperties
  val errorMessage = "qb.max.props.violated"
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
  val errorMessage = "qb.format." + format
}

/**
 * Rule that checks whether a string matches the format of a datetime, that is, whether it can be parsed the
 * Joda DateTime class.
 */
object DateTimeRule extends FormatRule[JsString] {
  val format = "date-time"
  def isValid(str: JsString) = Try(new DateTime(str.value)).isSuccess
}

/**
 * Rule that checks whether a string matches the format of a POSIX time, that is, whether it number
 * is whole at positive.
 */
object PosixTimeRule extends FormatRule[JsNumber] {
  val format = "posix-time"
  def isValid(d: JsNumber) = d.value.toDouble.isWhole && d.value.toDouble > 0
}
