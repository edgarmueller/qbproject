package org.qbproject.api.csv

import org.qbproject.api.csv.CSVColumnUtil.CSVRow
import java.io.InputStream
import play.api.libs.json._
import scala.util.Success
import org.qbproject.csv._
import org.qbproject.api.schema._
import org.qbproject.api.schema.QBSchema.{resolvePath => qbSchemaResolvePath, _}
import org.qbproject.csv.ResourceReference
import play.api.libs.json.JsArray
import play.api.libs.json.JsSuccess
import org.qbproject.csv.ResourceMapping
import scala.util.Success
import play.api.libs.json.JsObject
import org.qbproject.api.csv.CSVColumnUtil.CSVRow
import play.api.data.validation.ValidationError

object CSVAdapter {

  def apply(pathConstructors: (PathSpec, PartialFunction[Any, JsValue])*) =
    new CSVAdapter(toPathBuilders(pathConstructors))

  def toPathBuilders(pathBuilderSpecs: Seq[(PathSpec, PartialFunction[Any, JsValue])]): Map[String, CSVRow => JsValue] = {
    pathBuilderSpecs.map { pathBuilderSpec =>
      pathBuilderSpec._1.schemaPath -> {
        row: CSVRow =>
          // TODO: resolve should be able to handle specified schem path somehow
//          val csvPath = helper.resolvePath(__ \ ("/" + pathBuilderSpec._1.schemaPath))
          pathBuilderSpec._2(CSVColumnUtil.getColumnData(pathBuilderSpec._1.csvPath)(row))
      }
    }.toMap
  }
}

class CSVAdapter(_pathBuilders: Map[String, CSVRow => JsValue]) extends CSVSchemaAdapter {

  // TODO: important
  override def pathBuilders = {
    _pathBuilders.toMap
  }

  def parse(schema: QBType, resource: QBResource, joinKeys: Set[SplitForeignJoinKey] = Set.empty): List[JsResult[JsValue]] = {
    val parser = new CSVAdaptRowUtil(row => adapt(schema.asInstanceOf[QBClass])(row), joinKeys)
    parser.parse(resource, ';', '"')
  }

  def parse(mainResourceIdentifier: String, schema: QBClass)(resourceMapping: (String, ResourceReference)*)(provider: QBResourceSet): JsResult[List[JsValue]] = {
    parseResources(mainResourceIdentifier, schema)(resourceMapping.foldLeft[ResourceMapping](ResourceMapping())((mapping, attrWithRef) => {
      mapping.+((attrWithRef._1, attrWithRef._2))
    }))(provider)
  }

  /**
   * Mix in join keys into sub schemas.
   * @param schema
   * @param resourceMapping
   * @return
   */
  def injectJoinKeys(schema: QBClass, resourceMapping: ResourceMapping): Map[QBType, String] = {
    // TODO: exception
    resourceMapping.all.map { entry =>
      val attr = entry._1
      val (key, foreignKey) = entry._2.joinKeys.toTuple
      (schema.follow[QBType](attr) match {
        case cls: QBClass =>
          cls ++ (foreignKey ->  schema.follow[QBType](key))
        case arr: QBArray =>
          if (arr.items.asInstanceOf[QBClass].attributes.map(_.name).contains(foreignKey)) {
            // join key already in array contained type
            arr.items
          } else {
            // assumes array contains a class, where we can mix in the join key
            arr.items match {
              case cls: QBClass => cls ++
                (foreignKey ->  schema.asInstanceOf[QBClass].follow[QBType](key))
              case _ =>
                throw new RuntimeException(s"Join key $foreignKey can not be mixed into " +
                  s"array containing primitive type.")
            }
          }
        case _ =>
          throw new RuntimeException(s"Join key $foreignKey can not be mixed into " +
            s"primitive type.")
      }) -> entry._2.resourceIdentifier
    }.toMap
  }

  def buildForeignSchemaMappings(schema: QBClass, resourceMapping: ResourceMapping, foreignResources: List[QBResource]): List[(QBType, QBResource)] = {
    injectJoinKeys(schema, resourceMapping).map(m =>
      m._1 -> foreignResources.map(r => r.identifier -> r).toMap.get(m._2).get
    ).toList
  }

  def parseResources(mainResourceIdentifier: String, schema: QBClass)(resourceMapping: ResourceMapping)(provider: QBResourceSet): JsResult[List[JsValue]] = {

    val attributesToJoinKeys = resourceMapping.attributesWithJoinKeys
    // filter out non multi join attributes
    val updatedSchema = schema -- (attributesToJoinKeys.collect {
      case a@(_, joinKey) if !joinKey.isInstanceOf[SplitJoinKey] => a
    }.map(_._1).toSeq:_*)

    val joinedResults = for {
      resolvedResource <- provider.get(mainResourceIdentifier)
      resolvedForeignResources <- sequenceJsResults[QBResource](resourceMapping.resourceIdentifiers.map(provider.get))
      results <- parseAll(
        // TODO
        (updatedSchema, resolvedResource) :: buildForeignSchemaMappings(schema, resourceMapping, resolvedForeignResources),
        attributesToJoinKeys.map(_._2).filter(_.isInstanceOf[SplitForeignJoinKey]).asInstanceOf[List[SplitForeignJoinKey]]
      ).map { parsedResults =>
        val joinData = attributesToJoinKeys
          .zip(parsedResults.tail)
          .map(x => JoinData(x._1._1, x._1._2, x._2))
          .toList

        fold(parsedResults.head, joinData)
      }
    } yield results

    // TODO: perform validation?
    joinedResults
//    joinedResults.flatMap { results =>
//      val validated  = results.map(obj => QBValidator.validate(schema)(obj.asInstanceOf[JsObject]))
//      sequenceJsResults(validated)
//    }
  }

  /**
   * WIP
   */
  // TODO: casts
  def fold(initData: List[JsValue], foreignData: List[JoinData]): List[JsValue] = {

    foreignData.foldLeft(initData)((objects, joinData) => {
      objects.map {obj =>
        val jsObject = obj.asInstanceOf[JsObject]
        val objectId  = jsObject \ joinData.keys.key
        joinData.data.filter { data =>
          val foreignKey = data.asInstanceOf[JsObject] \ joinData.keys.foreignKey
          objectId match {
            case array: JsArray =>
              array.value.contains(foreignKey)
            case _ =>
              foreignKey == objectId
          }
        }.map(_.asInstanceOf[JsObject] - joinData.keys.foreignKey) match {
          case Nil  => jsObject
          case data => jsObject.deepMerge(Json.obj(joinData.attributeName -> data))
        }
      }
    })
  }

  // TODO: review joinKeys parameters, seems ugly
  def parseAll(resources: List[(QBType, QBResource)], joinKeys: List[SplitForeignJoinKey]): JsResult[List[List[JsValue]]] = {
    val results = for {
      (qbType, resource) <- resources
    } yield {
      val contents = parse(qbType, resource, joinKeys.toSet)
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

  case class QBValidationError(resource: QBResource, csvRow: Int, error: String) //extends ValidationError(error, resource, csvRow)

//  private def sequenceJsResultsPerResource[A](contents: List[JsResult[A]]): JsResult[List[A]] = {
//    if (!contents.exists(_.asOpt.isEmpty)) {
//      JsSuccess(contents.collect { case JsSuccess(result, _) => result })
//    } else {
//      JsError(contents.collect {
//        case JsError(err) => err
//      }.reduceLeft(_ ++ _)
//       .map(pathToError => {
//        pathToError._1 -> pathToError._2.map { _ match {
//            case ValidationError(msg, errorInfo: CSVErrorInfo) =>
//              QBValidationError(errorInfo.resource, errorInfo.csvRow, msg)
//            case v => v
//          }
//        }
//       })
//      )
//    }
//  }
}
