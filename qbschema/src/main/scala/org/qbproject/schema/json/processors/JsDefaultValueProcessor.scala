package org.qbproject.schema.json.processors

import play.api.libs.json._
import org.qbproject.schema._
import org.qbproject.schema.visitor.{Visitor, AnnotationProcessor, JsValueProcessor}
import org.qbproject.api.schema._
import org.qbproject.api.schema.QBBooleanImpl
import org.qbproject.api.schema.QBIntegerImpl
import org.qbproject.api.schema.QBDefaultAnnotation
import org.qbproject.api.schema.QBNumberImpl

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
      classOf[QBNumber] -> numberConverter,
      classOf[QBInteger] -> numberConverter,
      classOf[QBBoolean] -> booleanConverter
    )
  }
}


