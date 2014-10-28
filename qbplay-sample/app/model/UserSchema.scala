package model

import org.qbproject.forms._
import org.qbproject.mongo.objectId
import org.qbproject.schema.QBSchema.{qbClass, qbEmail, qbEnum, qbNonEmptyText}

object UserSchema {

    val user = qbClass(
      "id" -> objectId,
      "name" -> qbNonEmptyText,
      "status" -> qbEnum("ACTIVE", "PASSIVE"),
      "email" -> qbEmail)

    val userView = QBViewModel(
      user,
      QBViewControl("name", QBViewPath("name")),
      QBViewControl("status", QBViewPath("status")),
      QBViewControl("email", QBViewPath("email")))

}
