package org.qbproject.schema.internal.json

import org.qbproject.schema.QBType
import org.qbproject.schema.internal.visitor._
import play.api.libs.json._

import scala.reflect.ClassTag

/**
 * A JsValueProcessor that finds all types and paths for which the matcher evaluates to true and modifies them via the map
 * method. In contrast to the JsTypeMapper this class acts as a builder and allows to specify multiple mappings
 * at once as well as passing in predicates as matching functions.
 *
 * @param schema
 *               a QB schema
 */
case class JsValueUpdateBuilder(schema: QBType, mappings: List[(QBType => Boolean, PartialFunction[JsValue, JsValue])] = List.empty) {

  val processor = new JsValueProcessor() {
    override def ignoreMissingFields = true
  }

  /**
   * Allows to created a modified version of the passed JsObject by passing in
   * partial functions that are called if the desired type is encountered.
   *
   * @param updater
   *              the partial function that describes how to modify the matched type
   * @return a JsResult containing the possibly modified JsObject
   */
  def byType[A <: QBType : ClassTag](updater: PartialFunction[JsValue, JsValue]): JsValueUpdateBuilder = {
    val clazz = implicitly[ClassTag[A]].runtimeClass
    val matcher = (q: QBType) => q.getClass.getInterfaces.contains(clazz) || q.getClass == clazz
    new JsValueUpdateBuilder(schema, (matcher -> updater) :: mappings)
  }

  /**
   * Allows to created a modified version of the passed JsObject by passing in
   * partial functions that are called if the desired type is encountered.
   *
   * @param updater
   *              the partial function that describes how to modify the matched type
   * @return a JsResult containing the possibly modified JsObject
   */
  def byTypeAndPredicate[A <: QBType : ClassTag](predicate: A => Boolean)(updater: PartialFunction[JsValue, JsValue]): JsValueUpdateBuilder = {
    val clazz = implicitly[ClassTag[A]].runtimeClass
    val matcher = (q: QBType) =>  ( q.getClass.getInterfaces.contains(clazz) || q.getClass == clazz) && predicate(q.asInstanceOf[A])
    new JsValueUpdateBuilder(schema, (matcher -> updater) :: mappings)
  }

  /**
   * Allows to created a modified version of the passed JsObject by passing in
   * partial functions that are called if the matcher evaluates to true
   *
   * @param updater
   *              the partial function that describes how to modify the matched type
   * @return a JsResult containing the possibly modified JsObject
   */
  def byPredicate(matcher: QBType => Boolean)(updater: PartialFunction[JsValue, JsValue]): JsValueUpdateBuilder =
    new JsValueUpdateBuilder(schema, (matcher -> updater) :: mappings)

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
   * Returns all matched paths together with their QB type.
   *
   * @param input
   *              the instance that is supposed to comply to the schema
   * @return a Seq containing tuples of the matched type and its JsPath
   */
  def matchedPaths(input: JsValue): Seq[(QBType, JsPath)] = {
    processor.process(schema, QBPath(), input, JsFilterVisitor(matcher))
             .getOrElse(List.empty).map(pair => pair._1 -> pair._2.toJsPath)
  }

  /**
   * Executes the mapping.
   *
   * @param input
   *              the JsObject that should be modified
   * @return a JsResult containing the possibly modified JsObject
   */
  def go(input: JsObject): JsObject = {

    val mp: Seq[(QBType, JsPath)] = matchedPaths(input)

    mp.foldLeft(input)((obj, pair) => {
      val updater = getByType(pair._1).get._2
      get(pair._2, obj) match {
        case JsError(_) => input
        case JsSuccess(value, _) => updated(pair._2.path, obj, updater(value)).asInstanceOf[JsObject]
      }
    })
  }

  private def get(path: JsPath, obj: JsValue): JsResult[JsValue] = {
    obj.transform(path.json.pick)
  }


  private def updated(nodes: Seq[play.api.libs.json.PathNode], json: JsValue, newValue: JsValue): JsValue = {
    nodes.headOption match {
      case Some(node) => node match {
        case KeyPathNode(key) => (json \ key).toOption.fold(json)(value =>
          json.asInstanceOf[JsObject] ++ Json.obj(key ->  updated(nodes.tail, value, newValue))
        )
        case IdxPathNode(idx) =>
          val arr = json.asInstanceOf[JsArray]
          val value = arr.value(idx)
          JsArray(arr.value.take(idx) ++ Seq(updated(nodes.tail, value, newValue)) ++ arr.value.drop(idx + 1))
      }
      case None => newValue
    }
  }
}
