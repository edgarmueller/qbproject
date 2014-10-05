package org.qbproject

import org.qbproject.schema.internal.QBSchemaUtil
import play.api.data.validation.ValidationError
import play.api.libs.json._


package object schema {

  type ERRORS = List[ValidationError]

  implicit def toJsObject(qbJs: QBJson): JsObject = qbJs.json
  implicit def toQBObject(qbJs: QBJson): QBClass = qbJs.schema

  implicit class QBTypeExtensionOps(qbType: QBType) {
    def prettyPrint: String = QBSchemaUtil.prettyPrint(qbType)
  }

  implicit class QBClassExtensionOps(qbClass: QBClass) {
    def hasAttribute(attributeName: String) = qbClass.attributes.exists(_.name == attributeName)
  }
}