package org.qbproject.csv

import org.qbproject.api.schema._
import org.qbproject.api.schema.QBSchema._
import org.qbproject.csv._
import play.api.libs.json._
import play.api.libs.json.JsArray
import play.api.libs.json.JsSuccess
import play.api.libs.json.JsObject
import org.qbproject.api.csv.QBResource
import org.qbproject.api.csv.QBResourceSet
import play.api.libs.json.Json.toJsFieldJsValueWrapper
import org.qbproject.csv.CSVColumnUtil.CSVRow

trait CSVAdapter extends CSVSchemaAdapter {

  type AttributeName = String

  def parse(schema: QBType, resource: QBResource,
    foreignSplitKeys: Set[ForeignSplitKey] = Set.empty): List[JsResult[JsValue]] = {

    val parser = new CSVAdaptRowUtil(row => adapt(schema.asInstanceOf[QBClass])(row), foreignSplitKeys)
    parser.parse(resource, ';', '"')
  }

  def parse(mainResourceIdentifier: String, schema: QBClass)(resourceMapping: (String, ResourceReference)*)(resourceSet: QBResourceSet): JsResult[List[JsValue]] = {

    def createResourceMapping = resourceMapping.foldLeft[ResourceMapping](ResourceMapping())((mapping, attrWithRef) => {
      mapping + (attrWithRef._1 -> attrWithRef._2)
    })

    val result = parseResources(mainResourceIdentifier, schema)(createResourceMapping)(resourceSet)
    resourceSet.close()
    result
  }

  /**
   * Mix in join keys into sub schemas.
   */
  private def injectJoinKeys(schema: QBClass, resourceMapping: ResourceMapping): List[QBType] = {
    resourceMapping.map { mappedResource =>
      val (key, foreignKey) = mappedResource.resourceRef.joinKeys.toTuple
      schema.follow[QBType](mappedResource.attributeName) match {
        case cls: QBClass => cls ++ (foreignKey -> schema.follow[QBType](key))
        case arr: QBArray => arr.items match {
          // join key already in array contained type
          case c: QBClass if c.attributes.map(_.name).contains(foreignKey) =>
            arr.items
          // assumes array contains a class, where we can mix in the join key
          case c: QBClass => c ++ (foreignKey -> schema.asInstanceOf[QBClass].follow[QBType](key))
          case _ => throw new RuntimeException(s"Join key $foreignKey can not be mixed into " +
            s"array containing primitive type.")
        }
        case _ => throw new RuntimeException(s"Join key $foreignKey can not be mixed into " +
          s"primitive type.")
      }
    }
  }

  private def buildForeignSchemaMappings(schema: QBClass, resourceMapping: ResourceMapping,
    foreignResources: List[QBResource]): List[(QBType, QBResource)] = {

    val foreignResourcesMap = foreignResources.map(resource => resource.identifier -> resource).toMap
    injectJoinKeys(schema, resourceMapping)
      .zip(resourceMapping.resourceIdentifiers)
      .map(m => m._1 -> foreignResourcesMap.get(m._2).get)
      .toList
  }

  def keepSplitKeys(schema: QBClass, resourceMapping: ResourceMapping) = {
    schema -- resourceMapping.allExceptSplitKeys.map(_._1)
  }

  def parseResources(mainResourceIdentifier: String, schema: QBClass)(resourceMapping: ResourceMapping)(provider: QBResourceSet): JsResult[List[JsValue]] = {

    val updatedSchema = keepSplitKeys(schema, resourceMapping)

    for {
      resolvedResource <- provider.get(mainResourceIdentifier)
      resolvedForeignResources <- sequenceJsResults[QBResource](resourceMapping.resourceIdentifiers.map(provider.get))
      foreignSchemas = buildForeignSchemaMappings(schema, resourceMapping, resolvedForeignResources)
      results <- parseAll(
        (updatedSchema, resolvedResource) :: foreignSchemas,
        resourceMapping.foreignSplitKeys.map(_._2)
      )
    } yield {
      val joinData = resourceMapping.map(mappedResource =>
        mappedResource.attributeName -> mappedResource.resourceRef.joinKeys
      ).zip(results.tail)
        .map(x =>
          JoinData(attributeName = x._1._1,
            keys = x._1._2,
            data = x._2)
        )

      fold(results.head.asInstanceOf[List[JsObject]], joinData)
    }
  }

  private def findMatchingData(targetId: JsValue, foreignData: JoinData): List[JsObject] = {
    // TODO: println & do we always want to remove the key from the foreign data
    foreignData.data.filter {
      case obj: JsObject =>
        val foreignKey = obj \ foreignData.keys.foreignKey
        targetId match {
          case array: JsArray =>
            array.value.contains(foreignKey)
          case _ =>
            foreignKey == targetId
        }
      case _ =>
        println("WARN: can not resolve target " + foreignData.keys.foreignKey +
          " on primitive value")
        false // in case target is not an object, ignore
    }.map(_.asInstanceOf[JsObject] - foreignData.keys.foreignKey)

  }

  private def fold(initData: List[JsObject], foreignData: List[JoinData]): List[JsValue] = {
    foreignData.foldLeft(initData)((joinTargets, foreignData) => {
      joinTargets.map { joinTarget =>
        val objectId = joinTarget \ foreignData.keys.key
        findMatchingData(objectId, foreignData) match {
          case Nil => joinTarget
          case data =>
            val attributePath = foreignData.attributeName.split('.').toList
            if (attributePath.size > 1) {
              deepJoin(joinTarget, attributePath, data)
            } else {
              joinTarget.deepMerge(Json.obj(foreignData.attributeName -> data))
            }
        }
      }
    })
  }

  private def deepJoin(jsObject: JsObject, pathToAttribute: List[String], data: List[JsObject]): JsObject = {
    pathToAttribute match {
      case attr :: Nil => jsObject.deepMerge(Json.obj(attr -> data))
      case attr :: path => jsObject \ attr match {
        case resolvedObject: JsObject => jsObject.deepMerge(Json.obj(attr -> deepJoin(resolvedObject, path, data)))
        case _ => throw new RuntimeException("Encountered non JsObject while joining at " +
          pathToAttribute.mkString("."))
      }
    }
  }

  private def parseAll(resources: List[(QBType, QBResource)],
    foreignSplitKeys: List[ForeignSplitKey]): JsResult[List[List[JsValue]]] = {

    val results = for {
      (qbType, resource) <- resources
    } yield {
      val contents = parse(qbType, resource, foreignSplitKeys.toSet)
      sequenceJsResults(contents)
    }
    sequenceJsResults(results.toList)
  }

  // TODO: duplicate code, we have this somewhere in the core, too
  private def sequenceJsResults[A](contents: List[JsResult[A]]): JsResult[List[A]] = {
    if (!contents.exists(_.asOpt.isEmpty)) {
      JsSuccess(contents.collect { case JsSuccess(result, _) => result })
    } else {
      JsError(contents.collect { case JsError(err) => err }.reduceLeft(_ ++ _))
    }
  }
}

object CSVAdapter {

  def apply(pathConstructors: (PathSpec, Any => JsValue)*) =
    new CSVAdapter{
	  override val pathBuilders = toPathBuilders(pathConstructors)
    }

  def toPathBuilders(pathBuilderSpecs: Seq[(PathSpec, Any => JsValue)]): Map[String, CSVRow => JsValue] = {
    pathBuilderSpecs.map { pathBuilderSpec =>
      pathBuilderSpec._1.schemaPath -> {
        row: CSVRow =>
          pathBuilderSpec._2(CSVColumnUtil.getColumnData(pathBuilderSpec._1.csvPath)(row))
      }
    }.toMap
  }
}
