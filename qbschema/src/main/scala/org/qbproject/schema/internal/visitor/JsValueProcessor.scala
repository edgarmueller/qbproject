package org.qbproject.schema.internal.visitor

import org.qbproject.schema._
import org.qbproject.schema.internal._
import play.api.data.validation.ValidationError
import play.api.libs.json.{JsArray, JsBoolean, JsNumber, JsObject, JsString, JsSuccess, _}

/**
 * <p>
 * A trait that encapsulates the logic of traversing an
 * JSON AST based on a given schema.
 * </p>
 *
 * <p>
 * The JsValueProcessor trait allows to specify a visitor that defines the actual behavior
 * to be performed on the nodes.
 * </p>
 *
 * <p>
 * It also allows additional processors to be plugged in that can influence the result.
 * There are two types of processors, annotation-based ones and type-based ones.
 * Depending for which annotations/types the processors are registered, these will
 * be called if the value processors encounters the appropriate elements.
 * </p>
 *
 */
trait JsValueProcessor {

  /**
   * The annotation-based processors.
   */
  val annotationProcessors: Map[Class[_], AnnotationProcessor] = createAnnotationProcessors

  /**
   * The type-based processors.
   */
  val typeProcessors: Map[Class[_], TypeProcessor] = createTypeProcessors

  /**
   * Allows clients to register annotation-based processors.
   * By default, no processors are registered.
   *
   * @return a map containing annotation-based processors, where keys are based
   *         on annotation types and values are actual annotation-based processors
   */
  def createAnnotationProcessors: Map[Class[_], AnnotationProcessor] = Map.empty


  /**
   * Allows clients to register type-based processors.
   * By default, no processors are registered.
   *
   * @return a map containing type-based processors, where keys are based
   *         on QB types and values are actual type processors
   */
  def createTypeProcessors: Map[Class[_], TypeProcessor] = Map.empty

  /**
   * Top-level entry point for calling the value processor.
   *
   * @param schema
   *              the QB schema
   * @param input
   *              the JsValue instance that should be compared against the schema
   * @return a JsResult containing a result of type O
   */
  def process[O](schema: QBType)(input: JsValue)(implicit v: Visitor[O]): JsResult[O] = process(schema, QBPath(), input)(v)

  /**
   * Whether missing fields or array entries should emit an error.
   * The default is to emit an error if fields are missing, but
   * clients may override.
   *
   * @return true, if missing fields should be ignored, false otherwise
   */
  def ignoreMissingFields: Boolean  = false

  /**
   * Processor dispatch method.
   *
   * @param schema
   *             the current schema
   * @param path
   *             the current path
   * @param input
   *             the current JsValue
   * @return a JsResult containing a result of type O
   */
  def process[O](schema: QBType, path: QBPath, input: JsValue)(implicit v: Visitor[O]): JsResult[O] = {
    val r = typeProcessors.get(schema.getClass) match {
      case Some(processor) => processor.process(schema, input, path)
      case None => JsSuccess(input)
    }

    r.flatMap { value =>
      (schema, value) match {
        case (qbString: QBString,  jsString: JsString)  => processString(qbString, path, jsString)
        case (qbObject: QBClass,  jsObject: JsObject)  => processObject(qbObject, path, jsObject)
        case (qbArray:  QBArray,   jsArray:  JsArray)   => processArray(qbArray,   path, jsArray)
        case (qbBool:   QBBoolean, jsBool:   JsBoolean) => processBoolean(qbBool,  path, jsBool)
        case (qbNumber: QBNumber,  jsNumber: JsNumber)  => processNumber(qbNumber, path, jsNumber)
        case (qbInt:    QBInteger, jsInt:    JsNumber)  => processInteger(qbInt,   path, jsInt)
        case (_,                   jsUndefined: JsUndefined) => JsError(path.toJsPath, "qb.value.not.found")
        case _ => JsError(path.toJsPath, "qb.incompatible.types"
          + "[expected: " + schema.toString
          +     ", was: " + QBSchemaUtil.mapJsValueToTypeName(input) + "]")
      }
    }
  }

  /**
   * Process an integer.
   *
   * @param schema
   *               the schema of the matched integer
   * @param path
   *               the matched path
   * @param int
   *               the matched instance
   * @return a JsResult containing a result of type O
   */
  def processInteger[O](schema: QBInteger, path: QBPath, int: JsNumber)(implicit v: Visitor[O]): JsResult[O] =
    v.atPrimitive(schema, int, path)

  /**
   * Process a number.
   *
   * @param schema
   *               the schema of the matched number
   * @param path
   *               the matched path
   * @param number
   *               the matched instance
   * @return a JsResult containing a result of type O
   */
  def processNumber[O](schema: QBNumber, path: QBPath, number: JsNumber)(implicit v: Visitor[O]): JsResult[O] =
    v.atPrimitive(schema, number, path)

  /**
   * Visit a string.
   *
   * @param schema
   *               the schema of the matched string
   * @param path
   *               the matched path
   * @param str
   *               the matched instance
   * @return a JsResult containing a result of type O
   */
  def processString[O](schema: QBString, path: QBPath, str: JsString)(implicit v: Visitor[O]): JsResult[O] =
    v.atPrimitive(schema, str, path)

  /**
   * Process a boolean.
   *
   * @param schema
   *               the schema of the matched boolean
   * @param path
   *               the matched path
   * @param bool
   *               the matched instance
   * @return a JsResult containing a result of type O
   */
  def processBoolean[O](schema: QBBoolean, path: QBPath, bool: JsBoolean)(implicit v: Visitor[O]): JsResult[O] =
    v.atPrimitive(schema, bool, path)


  /**
   * Process an object.
   *
   * @param schema
   *             the schema of the object
   * @param path
   *             the current path
   * @param obj
   *             the matched JsObject
   * @return a JsResult containing a result of type O
   */
  def processObject[O](schema: QBClass, path: QBPath, obj: JsObject)(implicit v: Visitor[O]): JsResult[O] = {

    var hasErrors = false
    var validFields: List[(String, O)] = List.empty
    var errors: List[JsError] = List.empty
    val isConstrainedClass = schema.isInstanceOf[QBConstrainedClass]

    schema.attributes.foreach(attr => {
      val attrPath = path.append(QBKeyPathNode(attr.name))
      val jsValue = obj \ attr.name
      val maybeValue = if (jsValue.isInstanceOf[JsUndefined]) {
        None
      } else {
        Some(jsValue)
      }

      val modifiedValue: Option[JsValue] = attr.annotations.foldLeft(maybeValue) {
        (value, annotation) =>
          annotationProcessors.get(annotation.getClass) match {
            case Some(processor) => processor.process(attr, value, attrPath, obj)
            case None => value
          }
      }

      modifiedValue match {
        case None if attr.annotations.exists(_.isInstanceOf[QBOptionalAnnotation])=> // Attribute is optional
        case None => // annotation could not be handled gracefully, ignore attribute
          errors ::= JsError(attrPath.toJsPath, ValidationError("Couldn't find Attribute at " + attrPath.toString))
          if (!isConstrainedClass) {
            hasErrors = true
          }
        case Some(value) =>
          val result = process(attr.qbType, attrPath, value)

          if (result.asOpt.isDefined && !result.get.isInstanceOf[JsUndefined]) {
            validFields = (attr.name -> result.get) :: validFields
          } else {
            if (!isConstrainedClass) {
              hasErrors = true
              errors = result.asInstanceOf[JsError] :: errors
            }
          }
      }
    })

    if (hasErrors && !ignoreMissingFields) {
      JsError(errors.reverse.collect { case JsError(e) => e }.reduceLeft(_ ++ _))
    } else {
      v.atObject(schema, validFields.reverse, path, obj)
    }
  }

  /**
   * Process an array.
   *
   * @param schema
   *             the schema of the array
   * @param path
   *             the current path
   * @param arr
   *             the matched JsArray
   * @return a JsResult containing a result of type O
   */
  def processArray[O](schema: QBArray, path: QBPath, arr: JsArray)(implicit v: Visitor[O]): JsResult[O] = {
    // TODO: subject to be optimized like visitObject
    val elements = arr.value.indices.map(idx => {
      process(schema.items, path.append(QBIdxNode(idx)), arr.value(idx))
    })
    if (!elements.exists(_.asOpt.isEmpty)) {
      v.atArray(schema, elements.collect { case JsSuccess(res, _) => res }, path, arr)
    } else {
      JsError(elements.collect { case JsError(err) => err }.reduceLeft(_ ++ _))
    }
  }
}