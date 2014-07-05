package org.qbproject.schema.internal.json.processors

import play.api.libs.json._
import org.qbproject.schema.internal._
import org.qbproject.schema.internal.visitor.{Visitor, AnnotationProcessor, JsValueProcessor}
import org.qbproject.schema._
import org.qbproject.schema.QBBooleanImpl
import org.qbproject.schema.QBIntegerImpl
import org.qbproject.schema.QBDefaultAnnotation
import org.qbproject.schema.QBNumberImpl

trait JsDefaultValueProcessor extends JsValueProcessor[JsValue] { self: Visitor[JsValue] =>

  override def createAnnotationProcessors: Map[Class[_], AnnotationProcessor] = Map(
    classOf[QBOptionalAnnotation] -> new JsOptionalAnnotationProcessor(),
    classOf[QBDefaultAnnotation] -> new JsDefaultAnnotationProcessor()
  )

  // TODO: use trait instead of implementation classes as keys for the map
  override def createTypeProcessors = {
    val numberConverter = new JsStringToNumberTypeProcessor()
    val booleanConverter = new JsStringToBooleanTypeProcessor()
    Map(
      classOf[QBNumberImpl] -> numberConverter,
      classOf[QBIntegerImpl] -> numberConverter,
      classOf[QBBooleanImpl] -> booleanConverter
    )
  }
}


