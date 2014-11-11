package org.qbproject

import org.qbproject.schema.QBTypeExtensionOps
import org.qbproject.schema.internal.QBSchemaUtil
import play.api.data.validation.ValidationError
import play.api.libs.json._

import scala.reflect.ClassTag


package object schema {

  val isQBString = (qbType: QBType) => qbType.isInstanceOf[QBString]
  val isQBNumber = (qbType: QBType) => qbType.isInstanceOf[QBNumber]
  val isQBInteger = (qbType: QBType) => qbType.isInstanceOf[QBInteger]

  def isQBAnnotation[A <: QBAnnotation : ClassTag]: QBAnnotation => Boolean =
    (annotation: QBAnnotation) => implicitly[ClassTag[A]].runtimeClass.isInstance(annotation)

  val isQBOptionalAnnotation = isQBAnnotation[QBOptionalAnnotation]
  val isQBDefaultAnnotation = isQBAnnotation[QBDefaultAnnotation]
  val isQBReadOnlyAnnotation = isQBAnnotation[QBReadOnlyAnnotation]

  type ERRORS = List[ValidationError]

  implicit def toJsObject(qbJs: QBJson): JsObject = qbJs.json
  implicit def toQBObject(qbJs: QBJson): QBClass = qbJs.schema


  implicit class JsObjectExtensions(jsObject: JsObject) {
    def get(fieldName: String): Option[JsValue] = {
      val jsValue = jsObject \ fieldName
      if (jsValue.isInstanceOf[JsUndefined]) {
        None
      } else {
        Some(jsValue)
      }
    }
  }

  implicit class QBTypeExtensionOps(qbType: QBType) {
    def prettyPrint: String = QBSchemaUtil.prettyPrint(qbType)
  }

  implicit class QBClassExtensionOps(qbClass: QBClass) {
    def hasAttribute(attributeName: String) ={
      qbClass.attributes.exists(_.name == attributeName)
    }
  }
}