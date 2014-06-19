package org.qbproject.api.schema

import org.qbproject.schema._
import play.api.libs.json._
import scala.reflect.ClassTag

class QBSchemaOps extends QBBaseSchemaOps {

  /**
   * ----------------------------------------------------------
   * 	Schema Ops Extensions
   * ----------------------------------------------------------
   */
  // TODO: think about how to preserve types without explicit casting
  implicit class QBSchemaOps(schema: QBClass) {

    def follow[A <: QBType](path: QBStringPath): A = resolvePath[A](schema)(path)

    /**
     * Renames the field located at the given path.
     *
     * Example: Given an schema <code>obj("a" -> obj("b" -> integer))</code>
     * rename(List("a","b"), "c") will change the object to
     * <code>obj("a" -> obj("c" -> integer))</code>.
     */
    def rename(path: QBStringPath, newFieldName: String): QBClass = renameField(schema)(path, newFieldName)

    def rename(oldName: String, newName: String): QBClass = renameField(schema)(oldName, newName)

    /**
     * Retains all fields of the object at the given path based
     * on the name of the fields.
     */
    def keep(path: QBStringPath, fields: List[String]): QBClass = retain(schema)(path, fields)

    /**
     * Synonym for keep(QBStringPath, List[String]).
     */
    def keepAt(path: QBStringPath, fields: String*): QBClass = retain(schema)(path, fields)

    /**
     * Retains all fields of the object.
     */
    def keep(fields: String*): QBClass = retain(schema)("", fields)

    /**
     * Retains all fields of the object.
     */
    def keep(fields: List[String]): QBClass = retain(schema)("", fields)


    /**
     * Makes all values referenced by the given list of paths
     * optional.
     */
    def ?(paths: String*): QBClass = makeOptional(schema, paths.toList.map(string2QBPath))

    /**
     * Adds the given field to the object located at the path of this schema.
     */
    def +(path: String, attr: QBAttribute): QBClass = add(schema)(path, List(attr))

    /**
      * Adds the given field to the object.
      */
    def +(attr: QBAttribute): QBClass = add(schema)("", List(attr))

    /**
     * Merges the fields of the given objects with this schema.
     */
    def ++(obj: QBClass): QBClass = merge(schema, obj)

    /**
     * Adds the given fields to the object located at the path of the given object.
     */
    def ++(path: String, fields: QBAttribute*): QBClass = add(schema)(path, fields.toList)

    /**
     * Adds the given fields to the object.
     */
    def ++(fields: QBAttribute*): QBClass = add(schema)("", fields.toList)

    /**
     * Removes all fields from this schema that are part of the given object.
     */
    def --(obj: QBClass): QBClass = extract(schema, obj)

    /**
     * Removes the value referenced by the path within this schema.
     */
    def -(path: String): QBClass = remove(schema, toQBPaths(List(path)))

    /**
     * Removes all values that are referenced by the list of paths within this schema.
     */
    def --(paths: String*): QBClass = remove(schema, toQBPaths(paths.toList))

    def --(paths: List[String]): QBClass = remove(schema, toQBPaths(paths))

    /**
     * Makes all values referenced by the given list of paths
     * read-only.
     */
    def readOnly(paths: String*) = makeReadOnly(schema, toQBPaths(paths.toList))

    /**
     * Map over the schema by type.
     *
     * @param modifier
     * the modifier that is to be applied onto matched attributes
     * @tparam A
     * the type to be matched
     * @return the modified schema
     */
    def map[A <: QBType : ClassTag](modifier: QBType => QBType): QBClass = {
      val clazz = implicitly[ClassTag[A]].runtimeClass
      mapTypesByPredicate(schema)(qbType => clazz.isInstance(qbType))(modifier).asInstanceOf[QBClass]
    }

    // TODO: caller must cast to object
    def mapOverAttributes(predicate: QBAttribute => Boolean)(modifier: QBAttribute => QBAttribute) =
      mapAttributes(schema)(predicate)(modifier)

    def foldOverAttributes[A](matcher: QBType => Boolean)(modifier: (QBAttribute, JsPath) => A): Seq[A] =
      foldAttributesByTypeWithPath[A](matcher)(schema, JsPath(), List.empty)(modifier)

      /**
       * Map over the schema by predicate.
       *
       * @param predicate
     * the predicate that is used for matching
       * @param modifier
     * the modifier that is to be applied onto matched attributes
       * @return the modified schema
     */
    def map(predicate: QBType => Boolean, modifier: QBType => QBType): QBClass =
      mapTypesByPredicate(schema)(predicate)(modifier).asInstanceOf[QBClass]

    /**
     * Compares the schema to another schema.
     *
     * @param otherSchema
     *           the schema to be compared against this one
     * @return true, if the schemas are equal, false otherwise
     */
    def isEquals(otherSchema: QBClass): Boolean =
      areEqual(schema, otherSchema)

    /**
     * Checks whether this schema is a subset of the given schema.
     *
     * @param otherSchema
     *           the schema that is supposed to contain this schema as a subset
     * @return true, if this schema is a subset of the given schema
     */
    def isSubSetOf(otherSchema: QBClass): Boolean =
      isSubSet(schema, otherSchema)

//    def adapt[A](adapter: (JsPath, QBType) => JsResult[JsValue]): JsResult[JsValue] =
//      adaptSchema(schema, JsPath(), adapter)
  }
}

