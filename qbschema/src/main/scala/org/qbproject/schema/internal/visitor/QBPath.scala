package org.qbproject.schema.internal.visitor

import play.api.libs.json.JsPath

/**
 *
 */
trait PathNode
case class QBKeyPathNode(s: String) extends PathNode {
  override def toString = s
}
case class QBIdxNode(idx: Int) extends PathNode

/**
 * Simple internal type for building up paths and converting them
 * to a JsPath eventually.
 * Note that for performance reasons the path is built up reversed,
 * but toJsPath returns the path in correct order.
 *
 * @param paths a list containing path nodes
 */
case class QBPath(paths: List[PathNode] = List.empty) {

  def :+(path: PathNode): QBPath = {
    QBPath(path :: paths)
  }

  def toJsPath: JsPath = {
    paths.reverse.foldLeft(JsPath())((p,s) => s match {
      case QBIdxNode(idx) => p(idx)
      case QBKeyPathNode(key) => p \ key
    })
  }

}
