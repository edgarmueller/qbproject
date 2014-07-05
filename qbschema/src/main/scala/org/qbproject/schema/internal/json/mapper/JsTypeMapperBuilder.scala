package org.qbproject.schema.internal.json.mapper

import org.qbproject.schema.internal.visitor._
import play.api.libs.json._
import play.api.libs.json.extensions.JsExtensions
import scala.reflect.ClassTag
import org.qbproject.schema.QBType

/**
 * A JsValueProcessor that finds all types and paths for which the matcher evaluates to true and modifies them via the map
 * method. In contrast to the JsTypeMapper this class acts as a builder and allows to specify multiple mappings
 * at once as well as passing in predicates as matching functions.
 *
 * @param schema
 *               a QB schema
 */
class JsTypeMapperBuilder(schema: QBType)
  extends JsValueProcessor[Seq[(QBType, QBPath)]]
  with JsTypeMapperVisitor {

  var mappings: List[(QBType => Boolean, PartialFunction[JsValue, JsValue])] = List.empty

  /**
   * Allows to created a modified version of the passed JsObject by passing in
   * partial functions that are called if the desired type is encountered.
   *
   * @param updater
   *              the partial function that describes how to modify the matched type
   * @return a JsResult containing the possibly modified JsObject
   */
  def map[A <: QBType : ClassTag](updater: PartialFunction[JsValue, JsValue]): JsTypeMapperBuilder = {
    val clazz = implicitly[ClassTag[A]].runtimeClass
    val matcher = (q: QBType) =>  q.getClass.getInterfaces.exists(_ == clazz) || q.getClass == clazz
    mappings = (matcher -> updater) :: mappings
    this
  }

  /**
   * Allows to created a modified version of the passed JsObject by passing in
   * partial functions that are called if the matcher evaluates to true
   *
   * @param updater
   *              the partial function that describes how to modify the matched type
   * @return a JsResult containing the possibly modified JsObject
   */
  def map(matcher: QBType => Boolean)(updater: PartialFunction[JsValue, JsValue]): JsTypeMapperBuilder = {
    mappings = (matcher -> updater) :: mappings
    this
  }

  // TODO: can not map onto same type twice -> test
  /**
   * @inheritdoc
   *
   * @param qbType
   *              a qbType
   * @return true, if the QB type is of interest, false otherwise
   */
  def matcher(qbType: QBType): Boolean = mappings.exists(_._1(qbType))

  private def getByType(qbType: QBType) = mappings.find(_._1(qbType))

  /**
   * @inheritdoc
   *
   * @return true, if missing fields should be ignored, false otherwise
   */
  override def ignoreMissingFields = true

  /**
   * Returns all matched paths together with their QB type.
   *
   * @param input
   *              the instance that is supposed to comply to the schema
   * @return a Seq containing tuples of the matched type and its JsPath
   */
  def matchedPaths(input: JsValue): Seq[(QBType, JsPath)] = {
    process(schema, QBPath(), input).getOrElse(List.empty).map(pair =>
      pair._1 -> pair._2.toJsPath)
  }

  /**
   * Executes the mapping.
   *
   * @param input
   *              the JsObject that should be modified
   * @return a JsResult containing the possibly modified JsObject
   */
  def go(input: JsObject): JsObject = {
    matchedPaths(input).foldLeft(input)((obj, pair) => {
      val updater = getByType(pair._1).get._2
      obj.get(pair._2) match {
        case _: JsUndefined => obj
        case value => obj.set((pair._2, updater(value))).asInstanceOf[JsObject]
      }
    })
  }
}
