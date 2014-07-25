package org.qbproject.schema.internal

import play.api.libs.json._
import scala.annotation.tailrec
import scala.reflect.ClassTag
import scalaz._
import Scalaz._
import org.qbproject.schema._

trait QBBaseSchemaOps {

  implicit class QBFieldExtensionOps(field: (String, QBType)) {
    def value = field._2
    def name = field._1
  }

  implicit def string2QBPath(str: String): QBStringPath = str.split("\\.").toList.filterNot(_.trim == "")

  def toQBPaths(paths: List[String]) = paths.toList.map(string2QBPath)

  type QBStringPath = List[String]

  def emptyPath: QBStringPath = List.empty

  def fail[A](msg: String) = throw new RuntimeException(msg)

  case class BuildDescription(descriptions: List[(QBClass, String)]) {
    def addDescription(obj: QBClass, field: String) = {
      BuildDescription(obj -> field :: descriptions)
    }

    def build(initialValue: QBType) = {
      if (descriptions.size > 0) {
        val (obj, fieldName) = descriptions.head
        val init = updateAttributeByPath(obj)(fieldName, _ => QBAttribute(fieldName, initialValue))
        descriptions.tail.foldLeft(init)((updated, desc) => {
          updateAttributeByPath(desc._1)(desc._2, _ => QBAttribute(desc._2, updated))
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

  /**
   * Resolves the given path on the given object, executes the modifier
   * if the path has been resolved and returns the updated object.
   *
   * @param resolvable
   *             the object which is supposed to contain an attribute path
   * @param path
   *             the path that is to be followed through the object
   * @param updateFn
   *             the update function
   * @tparam A
   *             the expected tyupe when the path is resolved
   * @return the updated object
   */
  def update[A <: QBType](resolvable: QBClass, path: QBStringPath, updateFn: A => QBType): QBClass = {
    val (buildDescription, value) = resolve(path, resolvable)
    val updated = updateFn(value.asInstanceOf[A])
    buildDescription.build(updated)
  }

  def attribute(obj: QBClass, fieldName: String): QBAttribute = {
    obj.attributes.find(_.name == fieldName).getOrElse(fail("field.does.not.exist [" + fieldName + "]"))
  }

  def updateAttributeByPath(root: QBClass)(path: QBStringPath, fn: QBAttribute => QBAttribute): QBClass = {
    val parentPath = path.init
    val attributeName = path.last
    update[QBClass](root, parentPath, obj => {
      val currentAttribute = attribute(obj, attributeName)
      obj.attributes.indexOf(currentAttribute) match {
        case -1  => fail("field.does.not.exist [" + attributeName + "]")
        case idx => QBClassImpl(obj.attributes.updated(idx, fn(currentAttribute)))
      }
    })
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

  def update(qbType: QBType)(predicate: QBType => Boolean)(modifier: QBType => QBType): QBType = {
    qbType match {
      case obj: QBClass =>
        val fields = obj.attributes.collect {
          case attr => QBAttribute(attr.name, update(attr.qbType)(predicate)(modifier))
        }
        if (predicate(obj)) {
          modifier(QBClassImpl(fields))
        } else {
          QBClassImpl(fields)
        }
      case arr: QBArray => QBArrayImpl(update(arr.items)(predicate)(modifier))
      case q if predicate(q) => modifier(q)
      case q => q
    }
  }

  def updateAttributeByPredicate(qbType: QBType)(predicate: QBAttribute => Boolean)(modifier: QBAttribute => QBAttribute): QBType = {
    qbType match {
      case obj: QBClass =>
        QBClassImpl(obj.attributes.collect {
          case attr if predicate(attr) =>
            val modifiedAttribute = modifier(attr)
            modifiedAttribute.copy(qbType = updateAttributeByPredicate(modifiedAttribute.qbType)(predicate)(modifier))
          case attr => QBAttribute(attr.name, updateAttributeByPredicate(attr.qbType)(predicate)(modifier), attr.annotations)
        })
      case arr: QBArray => QBArrayImpl(updateAttributeByPredicate(arr.items)(predicate)(modifier))
      case q => q
    }
  }

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

  // TODO: think about splitting collapse function and/or rename to flatten
    def collapse[A <: QBType : ClassTag, B : Monoid](obj: QBClass)(modifier: QBAttribute => B): B = {

      def _collapse(obj: QBClass, result: B)(modifier: QBAttribute => B): B = {
        val clazz = implicitly[ClassTag[A]].runtimeClass
        obj.attributes.foldLeft(result)((res, attr) => attr.qbType match {
          case obj: QBClass if clazz.isInstance(obj) =>
            _collapse(obj, res |+| modifier(attr))(modifier)
          case obj: QBClass =>
            _collapse(obj, res)(modifier)
          case a if clazz.isInstance(a) => res |+| modifier(attr)
          case a => res
        })
      }

    val m = implicitly[Monoid[B]]
    _collapse(obj, m.zero)(modifier)
  }

  def collapseWithPath[B : Monoid](matcher: QBType => Boolean)(obj: QBClass)(modifier: (QBAttribute, JsPath) => B): B = {

    def _collapseWithPath(matcher: QBType => Boolean)(obj: QBClass, path: JsPath, result: B)(modifier: (QBAttribute, JsPath) => B): B = {
      obj.attributes.foldLeft(result)((res, attr) => attr.qbType match {
        case obj: QBClass if matcher(obj) =>
          _collapseWithPath(matcher)(obj, path \ attr.name, res |+| modifier(attr, path \ attr.name))(modifier)
        case obj: QBClass =>
          _collapseWithPath(matcher)(obj, path, result)(modifier)
        case a if matcher(a) => res |+| modifier(attr, path)
        case a => res
      })
    }

    val m = implicitly[Monoid[B]]
    _collapseWithPath(matcher)(obj, JsPath(), m.zero)(modifier)
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
    update[QBClass](root, path, obj => {
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
  def renameAttribute(root: QBClass)(path: QBStringPath, newFieldName: String): QBClass =
    updateAttributeByPath(root)(path, attr => QBAttribute(newFieldName, attr.qbType, attr.annotations))


  /**
   * Makes all values referenced by the given list of paths
   * optional.
   */
  def makeOptional(root: QBClass, paths: List[QBStringPath]): QBClass =
    paths.foldLeft(root)((obj, path) =>
      updateAttributeByPath(obj)(path, attr => attr.addAnnotation(QBOptionalAnnotation())))

  /**
   * Marks all values referenced by the given list of paths as read-only.
   *
   * @param schema
   *         the schema that is supposed to contain the attributes that are referenced by the given paths
   * @return the updated schema with the referenced attributes being marked as read-only
   */
  def makeReadOnly(schema: QBClass, paths: List[QBStringPath]): QBClass =
    paths.foldLeft(schema)((obj, path) =>
      updateAttributeByPath(obj)(path, attr => attr.addAnnotation(QBReadOnlyAnnotation())))

  /**
   * Returns the path of the given subschema, if it is contained in the schema.
   *
   * @param schema
   *          the schema that is supposed to contain the sub-schema
   * @param subSchema
   *          the sub-schema that is supposed to be contained in the first schema
   * @return the path of sub-schema in the schema, if it is contained, None otherwise
   */
  def pathOfSubSchema(schema: QBClass, subSchema: QBClass): Option[String] = {
    import Scalaz._
    collapseWithPath(_ => true)(schema)((attr, path) => if (attr.qbType == subSchema) Some(path.toString()) else None)
  }

  /**
   * Adds the given fields to the object located at the path of the given object.
   * 
   * @param schema
   *           the schema to which the attributes should be added
   * @param path 
   *           the path within the given schema at which the attributes should be added 
   * @param attributes
   *           the actual attributes to be added
   */
  def add(schema: QBClass)(path: QBStringPath, attributes: Seq[QBAttribute]): QBClass = {
    val fieldNames = attributes.map(_.name)
    update[QBClass](schema, path, obj => QBClassImpl(obj.attributes.filterNot(fd => fieldNames.contains(fd.name)) ++ attributes))
  }

  /**
   * Removes all values that are referenced by the list of paths within the given object.
   * 
   * @param schema
   *            the schema from which to remove attributes
   * @param paths
   *            the paths to the attributes that are to be removed
   */
  def remove(schema: QBClass, paths: Seq[QBStringPath]): QBClass =
    paths.foldLeft(schema)((obj, path) => {
      val objPath = if (path.size > 1) path.init else List("")
      val attributeName = if (path.size == 1) path.head else path.last
      update[QBClass](obj, objPath, o => {
        QBClassImpl(o.attributes.filterNot(_ == attribute(o, attributeName)))
      })
    })

  /**
   * Merges the attributes of the second schema into the first one.
   * 
   * @param schema
   *             the target schema
   * @param otherSchema
   *             the schema to be merged into the target schema
   */
  // TODO: duplicate check
  def merge(schema: QBClass, otherSchema: QBClass) = add(schema)(emptyPath, otherSchema.attributes)

  /**
   * Removes all attributes from the first schema that are also part of the second given schema.
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
  def areEqual(schema: QBClass, otherSchema: QBClass): Boolean =
    schema.equals(otherSchema)

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
    import std.anyVal.booleanInstance.disjunction
    implicit val M = disjunction
    subSchema.equals(schema) ||
      collapse[QBClass, Boolean](schema)(_.qbType.equals(subSchema))
  }
}