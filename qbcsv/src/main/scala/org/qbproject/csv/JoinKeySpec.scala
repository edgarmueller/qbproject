package org.qbproject.csv

/**
 *
 */
trait JoinKeySpec {
  /**
   * Returns the
   * @return
   */
  def key: String

  /**
   * Returns the foreign key.
   * @return the foreign key
   */
  def foreignKey: String

  /**
   * Returns this join spec as a tuple.
   * @return
   */
  def toTuple: (String, String) = (key, foreignKey)
}

case class JoinKey(key: String, foreignKey: String) extends JoinKeySpec
case class SplitJoinKey(key: String, foreignKey: String) extends JoinKeySpec


