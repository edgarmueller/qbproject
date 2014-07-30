package org.qbproject.csv.internal

import org.qbproject.csv._
import org.qbproject.csv.internal.CSVColumnUtil.CSVRow
import org.qbproject.schema._
import org.qbproject.schema.QBSchema._
import play.api.libs.json.Json.toJsFieldJsValueWrapper
import play.api.libs.json._

import scalaz.Scalaz._
import scalaz._

/**
 * Internal class for importing a CSV resource according to a given schema.
 */
class CSVImporter(separatorChar: Char = ';', quoteChar: Char = '"') extends CSVSchemaAdapter {

  protected def internalParse(schema: QBType, resource: QBResource)
                     (foreignSplitKeys: Set[ReverseSplitKey]): List[JsResult[JsValue]] = {
    val parser = new CSVAdaptRowUtil(row => adapt(schema.asInstanceOf[QBClass])(row), foreignSplitKeys)
    parser.parse(resource, separatorChar -> quoteChar)
  }

  /**
   * Import a single resource according to a given schema.
   *
   * @param schema
   *               the schema which the CSV data needs to conform to
   * @param resource
   *               the resource containing the CSV data to be imported
   * @return a list of JsResult that indicate which CSV data row could be parsed into a JsValue
   */
   def parse(schema: QBType, resource: QBResource): Either[QBCSVErrorMap, List[JsValue]] = {
    val jsResult = sequenceJsResults(internalParse(schema, resource)(Set.empty))
    jsResult match {
      case JsSuccess(values, _) => Right(values)
      case err: JsError => Left(QBCSVErrorMap(err))
    }
  }

  /**
   * Import multiple resource according to a given schema and a attribute to resource mapping that indicates
   * how data should be joined.
   *
   * @param mainResourceIdentifier
   *               the resource containing CSV data with foreign attributes
   * @param schema
   *               the schema which the main resource needs to conform to
   * @param resourceMapping
   *               a mapping describing how data should be joined. A mapping consists of a foreign attribute
   *               name and a resource.
   * @param resourceSet
   *               the resource set containing all the resources that are involved in the import.
   *               Each resource will be closed.
   */
   def parse(mainResourceIdentifier: String, schema: QBClass)
                     (resourceMapping: (String, ResourceReference)*)
                     (resourceSet: QBResourceSet): Either[QBCSVErrorMap, List[JsValue]] = {

    def createResourceMapping = resourceMapping.foldLeft(ResourceMapping())((resourceMapping, singleMapping) => {
      resourceMapping + (singleMapping._1 -> singleMapping._2)
    })

    val result = parseResources(mainResourceIdentifier, schema)(createResourceMapping)(resourceSet)
    resourceSet.close()
    result
  }

  private def parseResources(primaryResourceId: String, schema: QBClass)
                    (resourceMapping: ResourceMapping)
                    (resourceSet: QBResourceSet): Either[QBCSVErrorMap, List[JsValue]] = {

    def fetchPrimaryResource = resourceSet.get(primaryResourceId)
    def fetchSecondaryResources = resourceMapping.resourceIdentifiers
      .map(resourceSet.get)
      .sequence[({type l[X]=Validation[NonEmptyList[QBCSVError], X]})#l, QBResource]

    val updatedPrimarySchema = keepSplitKeys(schema, resourceMapping)

    val validationResult = for {
      resources <- (fetchPrimaryResource |@| fetchSecondaryResources){ _ :: _ }
      secondarySchemaMappings <- buildSecondarySchemaMappings(schema, resourceMapping, resources.tail)
      results <- parseAll(
        (updatedPrimarySchema, resources.head) :: secondarySchemaMappings,
        resourceMapping.reverseSplitKeys
      )
      result <- fold(results.head.asInstanceOf[List[JsObject]], buildJoinData(resourceMapping, results))
    } yield {
      result
    }

    validationResult match {
      case Success(js) => Right(js)
      case Failure(errors) => Left(new QBCSVErrorMap(errors.toList.groupBy(_.resourceId).toMap))
    }
  }

  private def buildJoinData(resourceMapping: ResourceMapping, results: List[List[JsValue]]): List[JoinData] = {
    val joinData = resourceMapping.map(mappedResource =>
      mappedResource.attributeName -> mappedResource.resourceRef.joinKeys
    ).zip(results.tail)
      .map {
      case ((attributeName, joinKeys), rowData) => JoinData(attributeName, joinKeys, rowData)
    }
    joinData
  }

  /**
   * Mix in join keys into sub schemas.
   */
  private def injectJoinKeys(schema: QBClass, resourceMapping: ResourceMapping): Validation[NonEmptyList[QBCSVError], List[QBType]] = {
    val x: List[Validation[NonEmptyList[QBCSVError], QBType]] = resourceMapping.map { mappedResource =>
      val (key, secondaryKey) = mappedResource.resourceRef.joinKeys.toTuple
      schema.resolve[QBType](mappedResource.attributeName) match {
        case cls: QBClass =>
          Success(cls ++ (secondaryKey -> schema.resolve[QBType](key)))
        case arr: QBArray => arr.items match {
          // join key already in array contained type
          case c: QBClass if c.attributes.map(_.name).contains(secondaryKey) =>
            arr.items.successNel
          // assumes array contains a class, where we can mix in the join key
          case c: QBClass =>
            (c ++ (secondaryKey -> schema.asInstanceOf[QBClass].resolve[QBType](key))).successNel
          case _ =>
            QBCSVJoinError(mappedResource.resourceRef.resourceIdentifier,
              s"Join key $secondaryKey can not be mixed into array containing primitive type."
            ).failNel
        }
        case _ =>
          QBCSVJoinError(mappedResource.resourceRef.resourceIdentifier,
            s"Join key $secondaryKey can not be mixed into primitive type."
          ).failNel
      }
    }
    x.sequence[({type l[X]=Validation[NonEmptyList[QBCSVError], X]})#l, QBType]
  }

  private def buildSecondarySchemaMappings(schema: QBClass, resourceMapping: ResourceMapping,
    secondaryResources: List[QBResource]): Validation[NonEmptyList[QBCSVError], List[(QBType, QBResource)]] = {

    val secondaryResourcesMap = secondaryResources.map(secondaryResource =>
      secondaryResource.identifier -> secondaryResource
    ).toMap

    for {
      updatedSecondarySchemas <- injectJoinKeys(schema, resourceMapping)
    } yield {
      updatedSecondarySchemas.zip(resourceMapping.resourceIdentifiers)
       .map(m => m._1 -> secondaryResourcesMap.get(m._2).get)
       .toList
    }
  }

  /**
   * Removes all attributes from the given schema except that are
   * specified as being a split key in the resource mapping.
   *
   * @param schema
   *               the schema
   * @param resourceMapping
   *               a resource mapping form which non split keys may be derived
   * @return a new schema with
   */
  private def keepSplitKeys(schema: QBClass, resourceMapping: ResourceMapping): QBClass = {
    def allExceptSplitKeys = resourceMapping.getAttributesBy(joinKey => !joinKey.isInstanceOf[SplitKey])
    schema -- allExceptSplitKeys
  }

  private def findMatchingData(targetId: JsValue, foreignData: JoinData): List[JsObject] = {
    // TODO: println & do we always want to remove the key from the foreign data
    foreignData.data.filter {
      case obj: JsObject =>
        val foreignKey = obj \ foreignData.keys.secondaryKey
        targetId match {
          case array: JsArray =>
            array.value.contains(foreignKey)
          case _ =>
            foreignKey == targetId
        }
      case _ =>
        println("WARN: can not resolve target " + foreignData.keys.secondaryKey +
          " on primitive value")
        false // in case target is not an object, ignore
    }.map(_.asInstanceOf[JsObject] - foreignData.keys.secondaryKey)

  }

  private def fold(initData: List[JsObject], foreignData: List[JoinData]): Validation[NonEmptyList[QBCSVError], List[JsObject]] = {
    foreignData.foldLeft[Validation[NonEmptyList[QBCSVError], List[JsObject]]](Success(initData))((joinTargets, secondaryData) => {
      joinTargets.flatMap(targets => {
        val x: List[Validation[NonEmptyList[QBCSVError], JsObject]] = targets.map { joinTarget =>
          if (joinTarget.fields.exists(_._1 == secondaryData.keys.primaryKey)) {
            val objectId = joinTarget \ secondaryData.keys.primaryKey
            findMatchingData(objectId, secondaryData) match {
              case Nil => joinTarget.successNel
              case data =>
                val attributePath = secondaryData.attributeName.split('.').toList
                if (attributePath.size > 1) {
                  deepJoin(joinTarget, attributePath, data)
                } else {
                  joinTarget.deepMerge(Json.obj(secondaryData.attributeName -> data)).successNel
                }
            }
          } else {
            QBCSVJoinError("TODO",
              s"Join target attribute ${secondaryData.keys.primaryKey} could not be " +
                s"resolved on object ${Json.prettyPrint(joinTarget)}"
            ).failNel
          }
        }
        x.sequence[({type l[X]=Validation[NonEmptyList[QBCSVError], X]})#l, JsObject]
      })
    })
  }

  /**
   * Performs a deep join, that is, a join based on a path consisting of multiple attributes instead
   * just a single one.
   *
   * @param jsObject
   *               the target object that may need to be resolved b
   * @param attributePath
   *               a path consisting of one or more attribute
   * @param data
   *               the data to be inserted at the target attribute
   * @return the target JSON object containing the joined data at the target attribute
   */
  // TODO: resolving attributes may fail -> what is expected behavior?
  private def deepJoin(jsObject: JsObject, attributePath: List[String], data: List[JsObject]): Validation[NonEmptyList[QBCSVError], JsObject] = {
    attributePath match {
      case attr :: Nil => jsObject.deepMerge(Json.obj(attr -> data)).successNel
      case attr :: path => if (!jsObject.fields.exists(_._1 == attr)) {
        QBCSVJoinError("TODO",
          s"Join target attribute $attr could not be found on object ${Json.prettyPrint(jsObject)}"
        ).failNel
      } else {
        jsObject \ attr match {
          case resolvedObject: JsObject =>
            deepJoin(resolvedObject, path, data).map(j => jsObject.deepMerge(Json.obj(attr -> j)))
          case _ => QBCSVJoinError("TODO",
            "Encountered non JsObject while joining at " + attributePath.mkString(".")
          ).failNel
        }
      }
    }
  }

  private def parseAll(resources: List[(QBType, QBResource)],
                       secondarySplitKeys: List[ReverseSplitKey]): Validation[NonEmptyList[QBCSVError], List[List[JsValue]]]  = {

    val results: List[Validation[NonEmptyList[QBCSVError], List[JsValue]]] = for {
      (qbType, resource) <- resources
    } yield {
      val contents = internalParse(qbType, resource)(secondarySplitKeys.toSet)
      sequenceJsResults(contents) match {
        case err: JsError =>
          val x = QBCSVErrorMap.bla(err)
          Failure(NonEmptyList(x.head, x.tail:_*))
        case JsSuccess(value, _) => value.successNel
      }
    }
    results.sequence[({type l[X]=Validation[NonEmptyList[QBCSVError], X]})#l, List[JsValue]]
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

object CSVImporter {

  def apply(pathConstructors: (PathSpec, Any => JsValue)*) =
    new CSVImporter {
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
