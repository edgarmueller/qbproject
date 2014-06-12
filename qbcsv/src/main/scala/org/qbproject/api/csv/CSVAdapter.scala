package org.qbproject.api.csv

import org.qbproject.api.csv.CSVColumnUtil.CSVRow
import java.io.InputStream
import play.api.libs.json._
import scala.util.Success
import org.qbproject.csv._
import org.qbproject.api.schema._
import org.qbproject.api.schema.QBSchema._
import org.qbproject.csv.ResourceReference
import play.api.libs.json.JsArray
import play.api.libs.json.JsSuccess
import org.qbproject.csv.ResourceMapping
import scala.util.Success
import play.api.libs.json.JsObject
import org.qbproject.api.csv.CSVColumnUtil.CSVRow

object CSVAdapter {

  def apply(pathConstructors: (PathSpec, PartialFunction[Any, JsValue])*) =
    new CSVAdapter(toPathBuilders(pathConstructors))

  def toPathBuilders(pathBuilderSpecs: Seq[(PathSpec, PartialFunction[Any, JsValue])]): Map[String, CSVRow => JsValue] =
    pathBuilderSpecs.map { pathBuilderSpec =>
      pathBuilderSpec._1.schemaPath -> {
        row: CSVRow =>
          pathBuilderSpec._2(CSVColumnUtil.getColumnData(pathBuilderSpec._1.csvPath)(row))
      }
    }.toMap
}

class CSVAdapter(_pathBuilders: Map[String, CSVRow => JsValue]) extends CSVSchemaAdapter {

  // TODO: important
  override def pathBuilders = {
    _pathBuilders.toMap
  }

  def parse(schema: QBType, inputStream: InputStream): List[JsResult[JsValue]] = {
    val parser = new CSVColumnUtil(row => adapt(schema.asInstanceOf[QBClass])(row))
    parser.parse(inputStream, ';', '"')(x => x.collect { case Success(s) => s } )
  }

  def parse(mainResourceIdentifier: String, schema: QBClass)(resourceMapping: (String, ResourceReference)*)(provider: QBResourceSet): JsResult[List[JsValue]] = {
    parseResources(mainResourceIdentifier, schema)(resourceMapping.foldLeft[ResourceMapping](ResourceMapping())((mapping, attrWithRef) => {
      mapping.+((attrWithRef._1, attrWithRef._2))
    }))(provider)
  }


  def injectJoinKeys(schema: QBClass, resourceMapping: ResourceMapping): Map[QBType, String] = {
    resourceMapping.all.map { entry =>
      val attr = entry._1
      val (key, foreignKey) = entry._2.joinKeys.toTuple
      (schema.follow[QBType](attr) match {
        case cls: QBClass => cls ++ (foreignKey ->  schema.follow[QBType](key))
        // TODO: simplification: this assumes that arrays are not deeply nested and that they contain objects
        case arr: QBArray =>
          arr.items.asInstanceOf[QBClass] ++
            (foreignKey ->  schema.asInstanceOf[QBClass].follow[QBType](key))
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

    for {
      resolvedResource <- provider.get(mainResourceIdentifier)
      resolvedForeignResources <- sequenceJsResults[QBResource](resourceMapping.resourceIdentifiers.map(provider.get))
      parsedResults <- parseAll((updatedSchema, resolvedResource) :: buildForeignSchemaMappings(schema, resourceMapping, resolvedForeignResources))
        .map { parsedResults =>
        val joinData = attributesToJoinKeys
          .zip(parsedResults.tail)
          .map(x => JoinData(x._1._1, x._1._2, x._2))
          .toList

        fold(parsedResults.head, joinData)
      }
    } yield parsedResults
  }


  /**
   * WIP
   */
  // TODO: casts
  def fold(initData: List[JsValue], foreignData: List[JoinData]): List[JsValue] = {

    println("foreign is "+ foreignData)
    foreignData.foldLeft(initData)((objects, joinData) => {

      objects.map {obj =>
        val jsObject = obj.asInstanceOf[JsObject]
        val objectId  = jsObject \ joinData.keys.key
        joinData.data.filter { data =>
          val foreignKey = data.asInstanceOf[JsObject] \ joinData.keys.foreignKey
          objectId match {
            case array: JsArray =>
              // TODO: foreign key is array?
              array.value.contains(foreignKey.asInstanceOf[JsArray].value.head)
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

  def parseAll(resources: Seq[(QBType, QBResource)]): JsResult[List[List[JsValue]]] = {
    parseAll(resources.map(resource => resource._1 -> resource._2.inputStream).toList)
  }

  def parseAll(resources: List[(QBType, InputStream)]): JsResult[List[List[JsValue]]] = {
    val results = for {
      resource <- resources
    } yield {
      val contents = parse(resource._1, resource._2)
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
