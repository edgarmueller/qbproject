package org.qbproject.schema.internal.visitor

import org.qbproject.schema._
import org.qbproject.schema.internal._
import play.api.data.validation.ValidationError
import play.api.libs.json._
import scalaz._
import Scalaz._

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
 * Note that for performance reasons processors need to be looked up via a Class.
 * </p>
 *
 * @param annotationProcessors a map containing annotation-based processors, where keys are based
 *        on annotation types and values are actual annotation-based processors.
 * @param typeProcessors a map containing type-based processors, where keys are based
 *         on QB types and values are actual type processors
 */
case class JsValueProcessor(annotationProcessors: Map[Class[_], AnnotationProcessor] = Map.empty,
                            typeProcessors:  Map[Class[_], TypeProcessor] = Map.empty) {

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


  private def applyAnnotationProcessors(attr: QBAttribute, attrPath: QBPath, obj: JsObject): Option[JsValue] = {
    val maybeValue = obj.get(attr.name)
    attr.annotations.foldLeft(maybeValue) {
      (value, annotation) =>
        annotationProcessors.get(annotation.getClass).map(processor =>
          processor.process(attr, value, attrPath, obj)
        ).getOrElse(value)
    }
  }

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

    val isConstrainedClass = schema.isInstanceOf[QBConstrainedClass]

    val (errors, validFields) = schema.attributes.foldLeft((List.empty[JsError], List.empty[(String, O)]))((acc, attr) => {
      val attrPath = path :+ QBKeyPathNode(attr.name)
      val modifiedValue: Option[JsValue] = applyAnnotationProcessors(attr, attrPath, obj)

      modifiedValue.fold {
        if (attr.annotations.exists(isQBOptionalAnnotation) || isConstrainedClass) {
          // TODO: log this error case since this is hard to debug
          acc
        } else {
          // TODO: is isConstrainedClass is true, add violation of constraint to error message
          ((errs: List[JsError]) =>
            JsError(attrPath.toJsPath, ValidationError("Missing attribute at " + attrPath.toString)) :: errs) <-: acc
        }
      } { value =>
        process(attr.qbType, attrPath, value) match {
          case JsSuccess(result, _) if !result.isInstanceOf[JsUndefined] => acc :-> ((attr.name -> result) :: _)
          case _: JsError if isConstrainedClass => acc
          case error: JsError => ((errs: List[JsError]) => error :: errs) <-: acc
        }
      }
    })

    if (errors.nonEmpty && !ignoreMissingFields) {
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
    val elements = arr.value.indices.map(idx => {
      process(schema.items, path :+ QBIdxNode(idx), arr.value(idx))
    })
    if (!elements.exists(_.asOpt.isEmpty)) {
      v.atArray(schema, elements.collect { case JsSuccess(res, _) => res }.toList, path, arr)
    } else {
      JsError(elements.collect { case JsError(err) => err }.reduceLeft(_ ++ _))
    }
  }
}