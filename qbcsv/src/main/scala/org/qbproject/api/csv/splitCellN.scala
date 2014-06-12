package org.qbproject.api.csv

import play.api.libs.json.JsValue
import org.qbproject.api.csv.CSVColumnUtil.CSVRow

//trait ArraySplitStrategy extends (CSVRow => JsValue) {
//  def header: String
////  def split(row: CSVRow): List[String]
////  def builder: List[String] => JsValue
////  override def apply(v1: String, v2: CSVRow): JsValue
//}
//
//// default is multiple value is one line
//// (multiples per line are not encoded in any strategy yet, but within the schema adapter)

class DefaultArrayPathBuilder(val header: String, _builder: => (List[String]) => JsValue) extends (CSVRow => JsValue) {

  // TODO: urghs
  def builder = _builder

  override def apply(row: CSVRow): JsValue = {
    // TODO: find is unsafe
    val p = if (header.contains("/")) {
      header.substring(header.toString().lastIndexOf("/") + 1)
    } else {
      header
    }

    println(p  + ", " + header + ", " + row.headers)
    val startidx = row.headers.find(_.contains(p)).map(matchedHeader => row.headers.indexOf(matchedHeader)).get // TODO: remove get
    val matchingHeaders = row.headers.drop(startidx).takeWhile { _.startsWith(p) }
    val bs = builder(CSVColumnUtil.getColumnRange(matchingHeaders)(row))

    bs
  }
}

//case class cell(val header: String)(_builder: => String => JsValue) extends ArraySplitStrategy {
//
//  override def apply(row: CSVRow): JsValue =
//    builder(row.getColumnData(header))
//
//  def builder = _builder
//
//}
//
//case class splitCell(val header: String, separator: String)(_builder: => (List[String]) => JsValue) extends ArraySplitStrategy {
//  override def apply(row: CSVRow): JsValue = builder(row.getColumnData(header).split(separator).toList)
//  def builder = _builder
//}
//
//case class splitCellN(val header: String, separator: String)(_builder: => (List[List[String]]) => JsValue) extends ArraySplitStrategy {
//
//  override def apply(row: CSVRow): JsValue = builder(
//    row.getColumnData(header)
//      .split("\n")
//      .toList
//      .map(_.split(separator).toList)
//  )
//
//  def builder = _builder
//
//}


