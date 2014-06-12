package org.qbproject.api.csv

import play.api.libs.json.JsValue
import org.qbproject.api.csv.CSVColumnUtil.CSVRow

class DefaultArrayPathBuilder(val header: String, builder: => (List[String]) => JsValue) extends (CSVRow => JsValue) {

  override def apply(row: CSVRow): JsValue = {
    val startIndex = row.headers.find(_.contains(header)).map(matchedHeader => row.headers.indexOf(matchedHeader)).get // TODO: remove get
    val matchingHeaders = row.headers.drop(startIndex).takeWhile { _.startsWith(header) }
    builder(CSVColumnUtil.getColumnRange(matchingHeaders)(row))
  }
}

