package org.qbproject.schema.internal.json.mapper

import org.qbproject.schema.QBType
import org.qbproject.schema.internal.visitor._
import play.api.libs.json._
import play.api.libs.json.extensions.JsExtensions

import scala.reflect.ClassTag

/**
 * Visitor that finds all types and paths for which the matcher evaluates to true and modifies them via the map
 * method.
 */
class JsValueUpdateProcessor[A <: QBType : ClassTag]()
  extends JsValueProcessor {

  /**
   * @inheritdoc
   *
   * @param qbType
   *             a qbType
   * @return true, if the QB type is of interest, false otherwise
   */
  def matcher(qbType: QBType): Boolean = {
    val clazz = implicitly[ClassTag[A]].runtimeClass
    clazz.isInstance(qbType)
  }

  /**
   * @inheritdoc
   *
   * @return true, if missing fields should be ignored, false otherwise
   */
  override def ignoreMissingFields = true

  /**
   * Returns all matched paths.
   *
   * @param schema
   *              a QB schema
   * @param input
   *              the instance that is supposed to comply to the schema
   * @return a JsResult containing all JsPaths that comply to the desired type
   */
  def matchedPaths(schema: QBType)(input: JsValue): JsResult[Seq[JsPath]] = {
    process(schema, QBPath(), input)(JsValueUpdateVisitor(matcher)).map(_.map(_._2).map(_.toJsPath))
  }

  /**
   * Allows to created a modified version of the passed JsObject by passing in
   * partial functions that are called if the desired type is encountered.
   *
   * @param schema
   *              a QB schema
   * @param input
   *              the instance that is supposed to comply to the schema
   * @param updater
   *              the partial function that describes how to modify the matched type
   * @return a JsResult containing the possibly modified JsObject
   */
  def map(schema: QBType)(input: JsObject)(updater: PartialFunction[JsValue, JsValue]): JsResult[JsObject] = {
    matchedPaths(schema)(input).map(
      _.foldLeft(input)((obj, path) => {
        obj.set((path, updater(obj.get(path)))).asInstanceOf[JsObject]
      }))
  }
}