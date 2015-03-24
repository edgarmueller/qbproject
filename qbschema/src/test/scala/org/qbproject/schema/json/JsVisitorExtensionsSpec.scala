package org.qbproject.schema.json

import org.qbproject.schema.internal.json.JsValidationVisitor
import org.qbproject.schema.internal.json.processors.JsDefaultValueProcessor
import org.qbproject.schema.internal._
import org.qbproject.schema._
import QBSchema._
import org.qbproject.schema.internal.visitor.{Visitor, QBPath, JsValueProcessor, AnnotationProcessor}
import play.api.libs.json.{Json, JsString, JsObject, JsValue}
import org.specs2.mutable.Specification
import org.qbproject.schema.internal.visitor.QBPath
import org.qbproject.schema.QBStringImpl
import play.api.libs.json.JsObject
import play.api.libs.json.JsString
import scala.Some

class ReadOnlyAnnotationVisitorExtension extends AnnotationProcessor {
  override def process(attr: QBAttribute, input: Option[JsValue], path: QBPath, jsObject: JsObject): Option[JsValue] = {
    input.fold[Option[JsValue]](None){ value =>
      attr.qbType match {
        case str: QBString => Some(JsString(value.asInstanceOf[JsString].value + "bar"))
        case x => input
      }
    }
  }
}


class JsVisitorExtensionsSpec extends Specification {

  val processor = JsDefaultValueProcessor(Map(classOf[QBReadOnlyAnnotation] -> new ReadOnlyAnnotationVisitorExtension))

  "Multiple annotations " should {
    "be respected" in {
      val schema = QBClassImpl(Seq(
        QBAttribute("s", QBStringImpl(), List(QBOptionalAnnotation(Some(JsString("foo"))), QBReadOnlyAnnotation()))
      ))
      val instance = Json.obj()
      val result = processor.process(schema)(instance, JsValidationVisitor())
      result.get \ "s" must beEqualTo(JsString("foobar"))
    }
  }

}
