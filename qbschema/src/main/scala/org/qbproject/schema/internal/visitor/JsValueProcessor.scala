package org.qbproject.schema.internal.visitor

import org.qbproject.schema._
import org.qbproject.schema.internal._
import play.api.data.validation.ValidationError
import play.api.libs.json._
import scala.collection.mutable.ListBuffer

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
  def process[O](schema: QBType)(input: JsValue, v: Visitor[O]): JsResult[O] = process(schema, QBPath(), input, v)

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
  def process[O](schema: QBType, path: QBPath, input: JsValue, visitor: Visitor[O]): JsResult[O] = {
    val result = typeProcessors.get(schema.getClass) match {
      case Some(processor) => processor.process(schema, input, path)
      case None => JsSuccess(input)
    }

    result.flatMap { value =>
      (value, schema) match {
        case (jsString: JsString, qbString: QBString)  => visitor.atPrimitive(qbString, jsString, path)
        case (jsBool:   JsBoolean,  qbBool: QBBoolean) => visitor.atPrimitive(qbBool, jsBool, path)
        case (jsNumber: JsNumber, qbNumber: QBNumber)  => visitor.atPrimitive(qbNumber, jsNumber, path)
        case (jsInt:    JsNumber,    qbInt: QBInteger) => visitor.atPrimitive(qbInt, jsInt, path)
        case (jsArray:  JsArray,   qbArray: QBArray)   => processArray(qbArray,   path, jsArray, visitor)
        case (jsObject: JsObject, qbObject: QBClass)   => processObject(qbObject, path, jsObject, visitor)
        case _ => JsError(path.toJsPath, "qb.incompatible.types"
          + "[expected: " + schema.toString
          +     ", was: " + QBSchemaUtil.mapJsValueToTypeName(input) + "]")
      }
    }
  }

  private def applyAnnotationProcessors(attr: QBAttribute, attrPath: QBPath, maybeValue: Option[JsValue], obj: JsObject): Option[JsValue] = {
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
  def processObject[O](schema: QBClass, path: QBPath, obj: JsObject, visitor: Visitor[O]): JsResult[O] = {

    val errors = ListBuffer.empty[Seq[(JsPath, Seq[ValidationError])]]
    val validFields = ListBuffer.empty[(String, O)]
    val isConstrainedClass = schema.isInstanceOf[QBConstrainedClass]

    schema.attributes.foreach { attr =>
      val attrPath = path :+ QBKeyPathNode(attr.name)
      val maybeValue = obj.get(attr.name)
      val modifiedValue: Option[JsValue] = applyAnnotationProcessors(attr, attrPath, maybeValue, obj)

      modifiedValue match {
        case None =>
          // TODO: is isConstrainedClass is true, add violation of constraint to error message
          if (!attr.annotations.exists(isQBOptionalAnnotation) && !isConstrainedClass) {
            errors += Seq(attrPath.toJsPath -> Seq(ValidationError("Missing attribute at " + attrPath.toString)))
          }
        case Some(value) => process(attr.qbType, attrPath, value, visitor) match {
          case JsSuccess(result, _) if !result.isInstanceOf[JsUndefined] => validFields += (attr.name -> result)
          case JsError(err) => if (!isConstrainedClass) { errors += err }
        }
      }
    }

    if (errors.nonEmpty && !ignoreMissingFields) {
      JsError(errors.reduceLeft(_ ++ _))
    } else {
      visitor.atObject(schema, validFields.toList, path, obj)
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
  def processArray[O](schema: QBArray, path: QBPath, arr: JsArray, visitor: Visitor[O]): JsResult[O] = {
    val elements = arr.value.zipWithIndex.map { case (jsValue, idx) =>
      process(schema.items, path :+ QBIdxNode(idx), jsValue, visitor)
    }
    if (elements.exists(_.isError)) {
      JsError(elements.collect { case JsError(err) => err }.reduceLeft(_ ++ _))
    } else {
      visitor.atArray(schema, elements.collect { case JsSuccess(res, _) => res }.toList, path, arr)
    }
  }
}