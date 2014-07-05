package org.qbproject.schema.internal

import play.api.libs.json._
import scala.annotation.tailrec
import scala.reflect.ClassTag
import play.api.libs.json.JsObject
import scalaz._
import org.qbproject.schema._
import play.api.libs.json.JsSuccess
import play.api.libs.json.JsObject

trait QBBaseSchemaOps {

  import Scalaz._

  implicit class QBFieldExtensionOps(field: (String, QBType)) {
    def value = field._2
    def name = field._1
  }

  implicit def string2QBPath(str: String): QBStringPath = str.split("\\.").toList.filterNot(_.trim == "")

  def toQBPaths(paths: List[String]) = paths.toList.map(string2QBPath)

  type QBStringPath = List[String]

  def emptyPath: QBStringPath = List.empty

  def fail[A](msg: String) = throw new RuntimeException(msg)

  case class BuildDescription(var descriptions: List[(QBClass, String)]) {
    def addDescription(obj: QBClass, field: String) = {
      BuildDescription(obj -> field :: descriptions)
    }

    def build(initialValue: QBType) = {
      if (descriptions.size > 0) {
        val (obj, fieldName) = descriptions.head
        val init = updateField(obj, fieldName, QBAttribute(fieldName, initialValue))
        descriptions.tail.foldLeft(init)((updated, desc) => {
          updateField(desc._1, desc._2, QBAttribute(desc._2, updated))
        })
      } else {
        initialValue.asInstanceOf[QBClass]
      }
    }
  }

  object BuildDescription {
    def emptyDescription = BuildDescription(List.empty)
  }

  // Core methods --

  def update[A <: QBType](path: QBStringPath, resolvable: QBClass, modifier: A => QBType): QBClass = {
    val (buildDescription, value) = resolve(path, resolvable, BuildDescription.emptyDescription)
    val updated = modifier(value.asInstanceOf[A])
    buildDescription.build(updated)
  }

  def attribute(obj: QBClass, fieldName: String): QBAttribute = {
    obj.attributes.find(_.name == fieldName).getOrElse(fail("field.does.not.exist [" + fieldName + "]"))
  }

  def updateField(obj: QBClass, attributeName: String, updatedAttribute: QBAttribute): QBClass = {
    val idx = obj.attributes.indexWhere(_.name == attributeName)
    if (idx == -1) {
      fail("field.does.not.exist [" + attributeName + "]")
    } else {
      QBClassImpl(obj.attributes.updated(idx, updatedAttribute))
    }
  }

  /**
   * Resolves the given path starting from the given resolvable.
   */
  def resolve(path: QBStringPath, resolvable: QBClass): (BuildDescription, QBType) =
    resolve(path, resolvable, BuildDescription.emptyDescription)

  @tailrec
  private def resolve(path: QBStringPath, resolvable: QBClass, buildDescription: BuildDescription): (BuildDescription, QBType) = {
    path match {
      case Nil =>
        (buildDescription, resolvable)
      case pathHead :: pathTail if pathHead.isEmpty => // safety check for root paths
        (buildDescription, resolvable)
      case pathHead :: pathTail =>

        if (pathHead.isEmpty) {
          (buildDescription, resolvable)
        } else {
          val field = resolvable.attributes.find(_.name == pathHead).getOrElse(fail("field.does.not.exist [" + pathHead + "]"))
          field.qbType match {
            case obj: QBClass => resolve(pathTail, obj, buildDescription.addDescription(resolvable, pathHead))
            case t => (buildDescription, t)
          }
        }
    }
  }

  //
  // Extension methods based on update --
  //

  def mapTypesByPredicate(qbType: QBType)(predicate: QBType => Boolean)(modifier: QBType => QBType): QBType = {
    qbType match {
      case obj: QBClass =>
        val fields = obj.attributes.collect {
          case attr => QBAttribute(attr.name, mapTypesByPredicate(attr.qbType)(predicate)(modifier))
        }
        if (predicate(obj)) {
          modifier(QBClassImpl(fields))
        } else {
          QBClassImpl(fields)
        }
      case arr: QBArray => QBArrayImpl(mapTypesByPredicate(arr.items)(predicate)(modifier))
      case q if predicate(q) => modifier(q)
      case q => q
    }
  }

  def mapAttributes(qbType: QBType)(predicate: QBAttribute => Boolean)(modifier: QBAttribute => QBAttribute): QBType = {
    qbType match {
      case obj: QBClass =>
        QBClassImpl(obj.attributes.collect {
          case attr if predicate(attr) =>
            val modifiedAttribute = modifier(attr)
            modifiedAttribute.copy(qbType = mapAttributes(modifiedAttribute.qbType)(predicate)(modifier))
          case attr => QBAttribute(attr.name, mapAttributes(attr.qbType)(predicate)(modifier), attr.annotations)
        })
      case arr: QBArray => QBArrayImpl(mapAttributes(arr.items)(predicate)(modifier))
      case q => q
    }
  }

  // TODO: do we need a generalized map function?
//  def mapSchema[A](schema: QBValue, modifier: QBValue => A): Tree[A] = {
//    schema match {
//      case obj: QBObject => Tree.node(modifier(obj), obj.fields.toStream.map(fd => mapSchema(fd.qbType, modifier)))
//      case arr: QBArray => Tree.node(modifier(arr), Stream.fill(1)(mapSchema(arr.items, modifier)))
//      case q => Tree.leaf(modifier(q))
//    }
//  }

  def adaptSchema[A](schema: QBType, path: JsPath, adapter: (JsPath, QBType) => JsResult[JsValue]): JsResult[JsValue] = {
    schema match {
      case obj: QBClass =>
        val fields = obj.attributes.map(fd => fd.name -> adaptSchema(fd.qbType, path \ fd.name, adapter))
          JsSuccess(JsObject(fields.collect {
            case (fieldName, JsSuccess(res, _)) if !res.isInstanceOf[JsUndefined] =>
              (fieldName, res)
          }))
      case q => adapter(path, q)
    }
  }

  def foldAttributesByType[A <: QBType : ClassTag, B](obj: QBClass, result: Seq[B])(modifier: QBAttribute => B): Seq[B] = {
    val clazz = implicitly[ClassTag[A]].runtimeClass
    obj.attributes.foldLeft(result)((res, attr) => attr.qbType match {
      case obj: QBClass if clazz.isInstance(obj) =>
        foldAttributesByType[A, B](obj, res :+ modifier(attr))(modifier)
      case obj: QBClass =>
        foldAttributesByType[A, B](obj, res)(modifier)
      case a if clazz.isInstance(a) => res :+ modifier(attr)
      case a => res
    })
  }

  def foldAttributesByTypeWithPath[B](matcher: QBType => Boolean)(obj: QBClass, path: JsPath, result: Seq[B])(modifier: (QBAttribute, JsPath) => B): Seq[B] = {
    obj.attributes.foldLeft(result)((res, attr) => attr.qbType match {
      case obj: QBClass if matcher(obj) =>
        foldAttributesByTypeWithPath[B](matcher)(obj, path \ attr.name, res :+ modifier(attr, path \ attr.name))(modifier)
      case obj: QBClass =>
        foldAttributesByTypeWithPath[B](matcher)(obj, path, res)(modifier)
      case a if matcher(a) => res :+ modifier(attr, path)
      case a => res
    })
  }

  /**
   * Resolves the given path.
   */
  def resolvePath[A <: QBType](obj: QBClass)(path: QBStringPath): A = {
    val result = resolve(path, obj, BuildDescription.emptyDescription)
    result._2.asInstanceOf[A]
  }

  /**
   * Retains all fields of the object at the given path based
   * on the name of the fields.
   */
  def retain(root: QBClass)(path: QBStringPath, fields: Seq[String]): QBClass = {
    update[QBClass](path, root, obj => {
      QBClassImpl(obj.attributes.filter(field => fields.contains(field.name)))
    })
  }

  /**
   * Renames the field located at the given path.
   *
   * Example: Given an schema <code>obj("a" -> obj("b" -> integer))</code>
   * rename(List("a","b"), "c") will change the object to
   * <code>obj("a" -> obj("c" -> integer))</code>.
   */
  // TODO: duplicate check
  def renameField(root: QBClass)(path: QBStringPath, newFieldName: String): QBClass =
    updateAttribute(root)(path, attr => QBAttribute(newFieldName, attr.qbType, attr.annotations))

  def updateAttribute(root: QBClass)(path: QBStringPath, fn: QBAttribute => QBAttribute): QBClass = {
    val parentPath = path.init
    val fieldName = path.last
    update[QBClass](parentPath, root, obj => {
      val currentAttribute = attribute(obj, fieldName)
      updateField(obj, currentAttribute.name, fn(currentAttribute))
    })
  }

  /**
   * Makes all values referenced by the given list of paths
   * optional.
   */
  def makeOptional(root: QBClass, paths: List[QBStringPath]): QBClass =
    paths.foldLeft(root)((obj, path) =>
      updateAttribute(obj)(path, attr => attr.addAnnotation(QBOptionalAnnotation())))

  /**
   * Makes all values referenced by the given list of paths
   * read-only.
   */
  def makeReadOnly(root: QBClass, paths: List[QBStringPath]): QBClass =
    paths.foldLeft(root)((obj, path) =>
      updateAttribute(obj)(path, attr => attr.addAnnotation(QBReadOnlyAnnotation())))

  /**
   * Adds the given fields to the object located at the path of the given object.
   */
  def add(root: QBClass)(path: QBStringPath, fields: Seq[QBAttribute]): QBClass = {
    val fieldNames = fields.map(_.name)
    update[QBClass](path, root, obj => QBClassImpl(obj.attributes.filterNot(fd => fieldNames.exists(_ == fd.name)) ++ fields))
  }

  /**
   * Removes all values that are referenced by the list of paths within the given object.
   */
  def remove(obj: QBClass, paths: Seq[QBStringPath]): QBClass =
    paths.foldLeft(obj)((currentObject, path) => {
      val pathToObject = path.init
      val field = path.last
      update[QBClass](pathToObject, currentObject, value => {
        val matched = attribute(value, field)
        QBClassImpl(value.attributes.filterNot(_ == matched))
      })
    })

  /**
   * Merges the fields of the second given object into the first one.
   */
  // TODO: duplicate check
  def merge(obj1: QBClass, obj: QBClass) = add(obj1)(emptyPath, obj.attributes)

  /**
   * Removes all fields from the first given object that are also part of the second given object.
   */
  def extract(obj1: QBClass, obj2: QBClass) = remove(obj1, obj2.attributes.map(field => string2QBPath(field.name)))

  /**
   * Compares the schema with each other
   *
   * @param schema
   *               the schema to be compared
   * @param otherSchema
     *             the schema to be compared against the first one
   * @return true, if the schemas are equal, false otherwise
   */
  def areEqual(schema: QBClass, otherSchema: QBClass): Boolean = {
    schema.equals(otherSchema)
  }

  /**
   * Checks whether the schema is a subset of the given schema.
   *
   * @param subSchema
   *               the schema that is supposed to be a subset
   * @param schema
   *               the schema to check the sub schema against
   * @return true, if the sub schema is a subset of the schema
   */
  def isSubSet(subSchema: QBClass, schema: QBClass): Boolean = {
    subSchema.equals(schema) ||
    foldAttributesByType[QBClass, Boolean](schema, List.empty)(_.qbType.equals(subSchema)).exists(_ == true)
  }

}

