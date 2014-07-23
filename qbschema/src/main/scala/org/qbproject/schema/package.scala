package org.qbproject

import play.api.libs.json._
import org.qbproject.schema._
import org.qbproject.schema.internal.QBSchemaUtil


package object schema {

  implicit def toJsObject(qbJs: QBJson): JsObject = qbJs.json
  implicit def toQBObject(qbJs: QBJson): QBClass = qbJs.schema

  implicit class QBTypeExtensionOps(qbType: QBType) {
    def prettyPrint: String = QBSchemaUtil.prettyPrint(qbType)
  }

  implicit class QBClassExtensionOps(qbClass: QBClass) {
    def hasAttribute(attributeName: String) = qbClass.attributes.exists(_.name == attributeName)
  }
}