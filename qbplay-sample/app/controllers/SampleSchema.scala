package controllers

import scala.collection.mutable.Map
import play.api.libs.json._
import org.qbproject.schema.QBSchema

object SampleSchema {

  import QBSchema._

  val project = qbClass(
    "id" -> qbString,
    "name" -> qbString(minLength(10), maxLength(20)),
    "title" -> default(qbString, JsString("hallo")),
    "bugs" -> qbList(qbString))

  val person = qbClass(
    "id" -> qbString,
    "name" -> qbString,
    "email" -> qbString,
    "age" -> qbNumber,
    "tags" -> qbList(qbString)
  )

  //  val state = Choice("open", "closed", "assigned")

  //  val comment : QBValue = obj(
  //    "author" -> string,
  //    "message" -> string,
  //    "replies" -> arr(comment)
  //  )

  val bug = qbClass(
    "id" -> qbString,
    "title" -> qbString(minLength(10)),
    //    "state" -> state,
    "reporter" -> qbString,
    "assignee" -> qbString,
    "comments" -> qbClass(
      "author" -> qbString,
      "message" -> qbString),
    "dueDate" -> qbNumber,
    "version" -> qbClass(
      "start" -> qbNumber,
      "end" -> qbNumber))

  val max = qbClass(
    "name" -> qbString,
    "age" -> qbNumber(min(10)),
    "brother" -> qbString
  ) - "name"

}