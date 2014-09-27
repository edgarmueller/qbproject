package org.qbproject.schema

import play.api.libs.json._

/**
 * Marker for all QBValue types.
 */
trait QBType

/**
 * Marker for all primitive QB types, which are numbers, strings and booleans.
 *
 * @tparam A
 *             the actual primitive type
 */
trait QBPrimitiveType[A <: JsValue] extends QBType with CompositeRule[A]

/**
 * Marker for base types.
 */
trait QBBaseType

/**
 * QBObject trait.
 */
trait QBClass extends QBType with QBBaseType with CompositeRule[JsObject] {
  /**
   * @inheritdoc
   *
   * @return a set of validation rules
   */
  def rules: Set[ValidationRule[JsObject]]

  /**
   * The fields of the object
   *
   * @return a sequence of QBAttributes
   */
  def attributes: Seq[QBAttribute]

  def apply(attributeName: String): Option[QBAttribute] = attributes.find(_.name == attributeName)

  override def toString = "object"
}

trait QBConstrainedClass extends QBClass {
  def values: Seq[QBClass]
}

class QBOneOfImpl(val values: Seq[QBClass])
  extends QBClassImpl(() => values.flatMap(_.attributes), Set(QBOneOfRule(values))) with QBConstrainedClass
class QBAllOfImpl(val values: Seq[QBClass])
  extends QBClassImpl(() => values.flatMap(_.attributes), Set(QBAllOfRule(values))) with QBConstrainedClass
class QBAnyOfImpl(val values: Seq[QBClass])
  extends QBClassImpl(() => values.flatMap(_.attributes), Set(QBAnyOfRule(values))) with QBConstrainedClass


/**
 * QBObject constructor.
 *
 * @param attrs
 *             the attributes of an object
 * @param rules
 *             optional rules an object must fulfill
 */
// This ain't a case class because we want fields to be evaluated lazily in order to being able to create
// recursive schemas
class QBClassImpl(attrs: () => Seq[QBAttribute], val rules: Set[ValidationRule[JsObject]] = Set.empty) extends QBClass {
  override val attributes = attrs()
  override def equals(other: Any): Boolean = other match {
    case that: QBClassImpl => that.attributes == attributes
    case _ => false
  }
}

/**
 * Companion object for QBObject.
 */
object QBClassImpl {

  /**
   * Creates an QBObject.
   *
   * @param attributes
   *           the attributes making up the object
   * @return the constructed object
   */
  def apply(attributes: => Seq[QBAttribute]) = new QBClassImpl(() => attributes, Set.empty)

  /**
   * Creates an QBObject.
   *
   * @param attributes
   *           the attributes making up the object
   * @param rules
   *           an optional set of rules that the QBObject being constructed must adhere
   * @return the constructed object
   */
  def apply(attributes: => Seq[QBAttribute], rules: Set[ValidationRule[JsObject]]) =
    new QBClassImpl(() => attributes, rules)
}

/**
 * Array
 */
trait QBArray extends QBType with QBBaseType with CompositeRule[JsArray] {
  val rules: Set[ValidationRule[JsArray]]
  def items: QBType
  override def toString = "array"
}

/**
 * Companion object for QBArray.
 */
class QBArrayImpl(qbType: () => QBType, val rules: Set[ValidationRule[JsArray]] = Set.empty) extends QBArray {
  lazy val items = qbType()
  override def equals(other: Any): Boolean = other match {
    case that: QBArrayImpl => that.items == items
    case _ => false
  }
}

/**
 * Companion object for QBArray.
 */
object QBArrayImpl {

  /**
   * Creates an QBArray.
   *
   * @param qbType
   *           the type that is contained by the array
   * @return the constructed object
   */
  def apply(qbType: => QBType) = new QBArrayImpl(() => qbType, Set.empty)

  /**
   * Creates an QBArray.
   *
   * @param qbType
   *           the type that is contained by the array
   * @param rules
   *           an optional set of rules that the QBArray being constructed must adhere
   * @return the constructed array
   */
  def apply(qbType: => QBType, rules: Set[ValidationRule[JsArray]]) =
    new QBArrayImpl(() => qbType, rules)
}

/**
 * String
 */
trait QBString extends QBPrimitiveType[JsString] with QBBaseType {
  val rules: Set[ValidationRule[JsString]]
  override def toString = "string"
}

case class QBStringImpl(rules: Set[ValidationRule[JsString]] = Set.empty) extends QBString

/**
 * Number
 */
trait QBNumber extends QBPrimitiveType[JsNumber] with QBBaseType  {
  val rules: Set[ValidationRule[JsNumber]]
  override def toString = "number"
}
case class QBNumberImpl(rules: Set[ValidationRule[JsNumber]] = Set.empty) extends QBNumber

/**
 * Integer
 */
trait QBInteger extends QBPrimitiveType[JsNumber] with QBBaseType  {
  val rules: Set[ValidationRule[JsNumber]]
  override def toString = "integer"
}
case class QBIntegerImpl(rules: Set[ValidationRule[JsNumber]] = Set.empty) extends QBInteger

/**
 * Boolean
 */
trait QBBoolean extends QBPrimitiveType[JsBoolean] with QBBaseType {
  val rules: Set[ValidationRule[JsBoolean]]
  override def toString = "boolean"
}
case class QBBooleanImpl(rules: Set[ValidationRule[JsBoolean]] = Set.empty) extends QBBoolean

import scalaz._

/**
 * ----------------------------------------------------------
 * 	Annotations
 * ----------------------------------------------------------
 */
trait QBAnnotation
case class QBAttribute(name: String, qbType: QBType, annotations: Seq[QBAnnotation] = Seq.empty) {
  def addAnnotation(annotation: QBAnnotation): QBAttribute =
    QBAttribute(name, qbType, annotation +: annotations)
}

case class QBDefaultAnnotation(value: JsValue) extends QBAnnotation
case class QBOptionalAnnotation(fallBack: Option[JsValue] = None) extends QBAnnotation
case class QBReadOnlyAnnotation() extends QBAnnotation

/**
 * DSL helper class
 */
case class AnnotatedQBType(qbType: QBType, annotations: Seq[QBAnnotation]) extends QBType

/**
 * ----------------------------------------------------------
 * 	Custom date types
 * ----------------------------------------------------------
 */
trait QBDateTime extends QBType {
  override def toString = "dateTime"
}
class QBDateTimeImpl(rules: Set[ValidationRule[JsString]]) extends QBStringImpl(rules) with QBDateTime

trait QBPosixTime extends QBType {
  override def toString = "posixTime"
}
class QBPosixTimeImpl(rules: Set[ValidationRule[JsNumber]]) extends QBNumberImpl(rules) with QBPosixTime

