package org.qbproject.csv

/**
 * Created by Edgar on 12.06.2014.
 */
trait PathSpec {
  def schemaPath: String
  def csvPath: String
}

case class Path(path: String) extends PathSpec{
  def schemaPath = path
  def csvPath = path
}

case class MappedPath(schemaPath: String, csvPath: String) extends PathSpec
