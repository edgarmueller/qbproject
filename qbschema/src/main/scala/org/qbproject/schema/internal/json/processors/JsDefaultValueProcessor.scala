package org.qbproject.schema.internal.json.processors

import org.qbproject.schema.{QBBooleanImpl, QBDefaultAnnotation, QBIntegerImpl, QBNumberImpl, _}
import org.qbproject.schema.internal.visitor.{AnnotationProcessor, JsValueProcessor}

trait JsDefaultValueProcessor extends JsValueProcessor {

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


