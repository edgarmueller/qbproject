package org.qbproject.csv

trait JoinKeySpec {

  def key: String

  def foreignKey: String

  def toTuple: (String, String) = (key, foreignKey)
}

case class JoinKey(key: String, foreignKey: String) extends JoinKeySpec
case class SplitJoinKey(key: String, foreignKey: String) extends JoinKeySpec
case class SplitForeignJoinKey(key: String, foreignKey: String) extends JoinKeySpec



