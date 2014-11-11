package org.qbproject.schema.internal.json.processors

import org.qbproject.schema.{QBBooleanImpl, QBDefaultAnnotation, QBIntegerImpl, QBNumberImpl, _}
import org.qbproject.schema.internal.visitor.{TypeProcessor, AnnotationProcessor, JsValueProcessor}

object JsDefaultValueProcessor {
  def apply() = new JsDefaultValueProcessor()
  def apply(annotationProcessors: Map[Class[_], AnnotationProcessor] = Map.empty,
            typeProcessors:  Map[Class[_], TypeProcessor] = Map.empty) =
    new JsDefaultValueProcessor(annotationProcessors, typeProcessors)
}

// TODO: use trait instead of implementation classes as keys for the map
class JsDefaultValueProcessor(annotationProcessors: Map[Class[_], AnnotationProcessor] = Map.empty,
                              typeProcessors:  Map[Class[_], TypeProcessor] = Map.empty) extends JsValueProcessor(
    Map(
      classOf[QBOptionalAnnotation] -> new JsOptionalAnnotationProcessor(),
      classOf[QBDefaultAnnotation] -> new JsDefaultAnnotationProcessor()
    ) ++ annotationProcessors,
    Map(
      classOf[QBNumberImpl] -> JsStringToNumberTypeProcessor,
      classOf[QBIntegerImpl] -> JsStringToNumberTypeProcessor,
      classOf[QBBooleanImpl] -> JsStringToBooleanTypeProcessor
    ) ++ typeProcessors
  )
