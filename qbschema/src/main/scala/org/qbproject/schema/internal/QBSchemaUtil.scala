package org.qbproject.schema.internal

import play.api.libs.json._
import org.qbproject.schema.{QBArray, QBClass, QBType}

object QBSchemaUtil {

  def isNotNull(input: JsValue) = input match {
    case _: JsNull.type => false
    case _ => true
  }

  /**
   * Used for formatting error messages.
   *
   * @param jsValue
   *           the JsValue which to create a string representation for
   * @return a string representation of the given JsValue
   */
  def mapJsValueToTypeName(jsValue: JsValue): String = jsValue match {
    case _: JsNumber => "number"
    case _: JsString => "string"
    case _: JsBoolean => "boolean"
    case _: JsObject => "object"
    case _: JsArray => "array"
    case JsNull => "null"
    case _ => "<no type>"
  }


  def prettyPrint(qbType: QBType, printAnnotations: Boolean = false, indent: Int = 0): String = qbType match {
    case obj: QBClass => "{\n" +
      obj.attributes.map { field =>
        " " * (indent + 2)  + field.name + ": " + (if (printAnnotations) field.annotations.mkString(",") else "") +
          prettyPrint(field.qbType, printAnnotations, indent + 2) + "\n"}.mkString +
        " " * indent + "}"
    case arr: QBArray => "[" + prettyPrint(arr.items, printAnnotations, indent) + "]"
    case q => q.toString
  }
}
