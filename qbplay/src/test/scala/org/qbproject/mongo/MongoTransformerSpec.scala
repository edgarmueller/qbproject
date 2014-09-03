package org.qbproject.mongo

import org.specs2.mutable.Specification
import play.api.libs.json.{JsArray, JsObject, JsString, Json}

import org.qbproject.schema.{QBClass, QBSchema}
import QBSchema._
import reactivemongo.bson.BSONObjectID

import org.joda.time.DateTime

class MongoTransformerSpec extends Specification {

  "MongoTransformer" should {

    val schema = qbClass("o" -> objectId, "d" -> qbDateTime, "e" -> qbPosixTime)
    val id = BSONObjectID.generate.stringify
    val date = new DateTime().toString()
    val time = System.currentTimeMillis() / 1000L

    "support writes" in {
      val i = Json.obj("o" -> id, "d" -> date, "e" -> time)
      val mongoTransformer = new MongoTransformer(schema)
      val result = mongoTransformer.toMongoJson(i)
      result.get must beEqualTo(Json.obj(
        "o" -> Json.obj("$oid" -> id),
        "d" -> Json.obj("$date" -> date),
        "e" -> Json.obj("$date" -> time)))
    }

    "support writes with multiple nested" in {
      val schema = qbClass("o" -> objectId, "d" -> qbDateTime, "e" -> qbPosixTime, "i" -> qbClass("x" -> objectId))
      val i = Json.obj("o" -> id, "d" -> date, "e" -> time, "i" -> Json.obj("x" -> id))
      val mongoTransformer = new MongoTransformer(schema)
      val result = mongoTransformer.toMongoJson(i)
      result.get must beEqualTo(Json.obj(
        "o" -> Json.obj("$oid" -> id),
        "d" -> Json.obj("$date" -> date),
        "e" -> Json.obj("$date" -> time),
        "i" -> Json.obj("x" -> Json.obj("$oid" -> id))))
    }

    "support reads" in {
      val i = Json.obj(
        "o" -> Json.obj("$oid" -> id),
        "d" -> Json.obj("$date" -> date),
        "e" -> Json.obj("$date" -> time))
      val expected = Json.obj("o" -> id, "d" -> date, "e" -> time)
      val mongoTransformer = new MongoTransformer(schema)
      val res = mongoTransformer.fromMongoJson(i)
      res.get must beEqualTo(expected)
    }

    "support reads with array" in {
      val schema = qbClass(
        "o" -> qbList(objectId),
        "d" -> qbDateTime,
        "e" -> qbPosixTime)

      val i = Json.obj(
        "o" -> Json.arr(Json.obj("$oid" -> id)),
        "d" -> Json.obj("$date" -> date),
        "e" -> Json.obj("$date" -> time))
      val expected = Json.obj("o" -> Json.arr(id), "d" -> date, "e" -> time)
      val mongoTransformer = new MongoTransformer(schema)
      val result = mongoTransformer.fromMongoJson(i)
      result.get must beEqualTo(expected)
    }

    "support nested reads" in {
      val schema = qbClass("o" -> objectId, "d" -> qbClass("o2" -> objectId), "e" -> qbPosixTime)

      val i = Json.obj(
        "o"  -> Json.obj("$oid" -> id),
        "d" -> Json.obj("o2" -> Json.obj("$oid" -> id)),
        "e" -> Json.obj("$date" -> time))
      val expected = Json.obj("o" -> id, "d" -> Json.obj("o2" -> id), "e" -> time)
      val mongoTransformer = new MongoTransformer(schema)
      mongoTransformer.fromMongoJson(i).get must beEqualTo(expected)
    }

    "support multiple, nested reads" in {

      val schema = qbClass("o" -> objectId, "d" -> qbDateTime, "e" -> qbPosixTime)


      val i = Json.obj(
        "o" -> Json.obj(
          "$oid" -> id
        ),
        "d" -> Json.obj("$date" -> date),
        "e" -> Json.obj("$date" -> time),
        "f" -> Json.obj(
          "g" -> Json.obj(
            "date" -> Json.obj("$date" -> date),
            "time" -> Json.obj("$date" -> time)
          )
        )
      )
      val expected = Json.obj(
        "o" -> id,
        "d" -> date,
        "e" -> time,
        "f" -> Json.obj(
          "g" -> Json.obj(
            "date" -> date,
            "time" -> time)))
      val mongoTransformer = new MongoTransformer(schema
        ++ qbClass(
        "f" -> qbClass(
          "g" -> qbClass(
            "date" -> qbDateTime,
            "time" -> qbPosixTime))))
      val result = mongoTransformer.fromMongoJson(i)

      result.asOpt.isDefined must beTrue
      result.get must beEqualTo(expected)
    }

    "support two same elements, which needs to be transformed" in {
      val input = Json.obj(
        "o" -> Json.obj(
          "$oid" -> id
        ),
        "d" -> Json.obj("$date" -> date),
        "e" -> Json.obj("$date" -> time),
        "f" -> Json.obj(
          "g" -> Json.obj(
            "date" -> Json.obj("$date" -> date),
            "date2" -> Json.obj("$date" -> date),
            "time" -> Json.obj("$date" -> time))))

      val expected = Json.obj(
        "o" -> id,
        "d" -> date,
        "e" -> time,
        "f" -> Json.obj(
          "g" -> Json.obj(
            "date" -> date,
            "date2" -> date,
            "time" -> time)))
      val mongoTransformer = new MongoTransformer(schema ++ qbClass(
        "f" -> qbClass(
          "g" -> qbClass(
            "date" -> qbDateTime,
            "date2" -> qbDateTime,
            "time" -> qbPosixTime))))

      val result = mongoTransformer.fromMongoJson(input)
      result.asOpt.isDefined must beTrue
      result.get must beEqualTo(expected)
    }

    val core = qbClass(
      "id" -> objectId,
      "lastModified" -> qbDateTime,
      "creationDate" -> qbDateTime)

    val companyCore = core ++ qbClass(
      "companyId" -> qbInteger,
      "companyStatus" -> qbEnum("active", "inactive"))

    val coordinates = qbClass(
      "lat" -> qbNumber,
      "lng" -> qbNumber)

    val companyInfo = qbClass(
      "name" -> qbString,
      "location" -> qbClass(
        "coordinates" -> coordinates))

    val dbCompany: QBClass = companyCore ++ qbClass(
      "company" -> companyInfo)

    "support write complex" in {
      val company = Json.obj(
        "id" -> id,
        "lastModified" -> date,
        "creationDate" -> date,
        "companyId" -> 1,
        "companyStatus" -> "active",
        "company" -> Json.obj(
          "name" -> "ACME",
          "location" -> Json.obj(
            "coordinates" -> Json.obj(
              "lat" -> 12.34,
              "lng" -> 23.45))))
      val mongoTransformer = new MongoTransformer(dbCompany)
      val jsValue = mongoTransformer.toMongoJson(company)
      jsValue.asOpt must beSome
    }

    "fail validation" in {
      val date = new DateTime().toString()

      val company = Json.obj(
        "id" -> BSONObjectID.generate.stringify,
        "lastModified" -> date,
        "creationDate" -> date,
        "companyId" -> 1,
        "companyStatus" -> "active",
        "company" -> Json.obj(
          "location" -> Json.obj(
            "lat" -> 12.34,
            "lng" -> 23.45)))
      val mongoTransformer = new MongoTransformer(schema)
      val jsValue = mongoTransformer.toMongoJson(company)
      jsValue.asOpt must beNone
    }

    "support optional writes with present field" in {
      val schema = qbClass("o" -> objectId, "d" -> optional(qbDateTime), "e" -> qbPosixTime)
      val instance = Json.obj("o" -> id, "d" -> date, "e" -> time)
      val mongoTransformer = new MongoTransformer(schema)
      val result = mongoTransformer.toMongoJson(instance)
      result.get must beEqualTo(Json.obj(
        "o" -> Json.obj("$oid" -> id),
        "d" -> Json.obj("$date" -> date),
        "e" -> Json.obj("$date" -> time)))
    }

    "support optional reads with present field" in {
      val schema = qbClass("o" -> objectId, "d" -> optional(qbDateTime), "e" -> qbPosixTime)
      val instance = Json.obj("o" -> Json.obj(
        "$oid" -> id),
        "d" -> Json.obj("$date" -> date),
        "e" -> Json.obj("$date" -> time))
      val expected = Json.obj("o" -> id, "d" -> date, "e" -> time)
      val mongoTransformer = new MongoTransformer(schema)
      mongoTransformer.fromMongoJson(instance).get must beEqualTo(expected)
    }

    "support optional writes with missing field" in {
      val schema = qbClass("o" -> objectId, "d" -> optional(qbDateTime), "e" -> qbPosixTime)
      val instance = Json.obj("o" -> id, "e" -> time)
      val mongoTransformer = new MongoTransformer(schema)
      val result = mongoTransformer.toMongoJson(instance)
      result.get must beEqualTo(Json.obj(
        "o" -> Json.obj("$oid" -> id),
        "e" -> Json.obj("$date" -> time)))
    }

    "support optional reads with present field" in {
      val schema = qbClass("o" -> objectId, "d" -> optional(qbDateTime), "e" -> qbPosixTime)
      val instance = Json.obj("o" -> Json.obj(
        "$oid" -> id),
        "e" -> Json.obj("$date" -> time))
      val expected = Json.obj("o" -> id, "e" -> time)
      val mongoTransformer = new MongoTransformer(schema)
      mongoTransformer.fromMongoJson(instance).get must beEqualTo(expected)
    }

    "support default writes with present field" in {
      val schema = qbClass("o" -> objectId, "d" -> default(qbDateTime, JsString(date)), "e" -> qbPosixTime)
      val instance = Json.obj("o" -> id, "d" -> date, "e" -> time)
      val mongoTransformer = new MongoTransformer(schema)
      val result = mongoTransformer.toMongoJson(instance)
      result.get must beEqualTo(Json.obj(
        "o" -> Json.obj("$oid" -> id),
        "d" -> Json.obj("$date" -> date),
        "e" -> Json.obj("$date" -> time)))
    }

    "support default reads with present field" in {
      val schema = qbClass("o" -> objectId, "d" -> default(qbDateTime, JsString(date)), "e" -> qbPosixTime)
      val instance = Json.obj("o" -> Json.obj(
        "$oid" -> id),
        "d" -> Json.obj("$date" -> date),
        "e" -> Json.obj("$date" -> time))
      val expected = Json.obj("o" -> id, "d" -> date, "e" -> time)
      val mongoTransformer = new MongoTransformer(schema)
      mongoTransformer.fromMongoJson(instance).get must beEqualTo(expected)
    }

    /**
     * Extension to MongoTransfomer
     */
    "rewrite _id to id" in {
      val schema = qbClass("id" -> objectId)
      val instance = Json.obj("_id" -> Json.obj("$oid" -> "52eb6c66e4b08a001831aa9a"))
      val expected = Json.obj("id" -> "52eb6c66e4b08a001831aa9a")
      val mongoTransformer = new MongoTransformer(schema)
      mongoTransformer.fromMongoJson(instance).get must beEqualTo(expected)
    }

    "rewrite id to _id" in {
      val schema = qbClass("id" -> objectId)
      val instance = Json.obj("id" -> "52eb6c66e4b08a001831aa9a")
      val expected = Json.obj("_id" -> Json.obj("$oid" -> "52eb6c66e4b08a001831aa9a"))
      val mongoTransformer = new MongoTransformer(schema)
      mongoTransformer.toMongoJson(instance).get must beEqualTo(expected)
    }

    "rewrite object to geo json" in {
      val schema = qbClass(
        "id" -> objectId,
        "loc" -> qbClass(
          "lat" -> qbNumber,
          "lng" -> qbNumber
        ))
      val instance = Json.obj(
        "id" -> "52eb6c66e4b08a001831aa9a",
        "loc" -> Json.obj(
          "lat" -> 40,
          "lng" -> 5
        )
      )
      val expected = Json.obj("_id" -> Json.obj(
        "$oid" -> "52eb6c66e4b08a001831aa9a"
      ),
        "loc" -> Json.obj(
          "type" -> "Point",
          "coordinates" -> Json.arr(40, 5)
        ))
      def toGeoJson(jsObject: JsObject): JsObject = JsObject(
        jsObject.fields.map(fd =>
          if (fd._1 == "loc") {
            fd._1 -> Json.obj(
              "type" -> "Point",
              "coordinates" -> Json.arr(
                fd._2.as[JsObject] \ "lat",
                fd._2.as[JsObject] \ "lng"
              )
            )
          } else {
            fd
          }
        )
      )
      val mongoTransformer = new MongoTransformer(schema, List(toGeoJson, toMongoId))
      val res = mongoTransformer.toMongoJson(instance)
      res.get must beEqualTo(expected)
    }

    "rewrite object to geo json" in {
      val schema = qbClass(
        "id" -> objectId,
        "loc" -> qbClass(
          "lat" -> qbNumber,
          "lng" -> qbNumber
        ))
      val expected = Json.obj(
        "id" -> "52eb6c66e4b08a001831aa9a",
        "loc" -> Json.obj(
          "lat" -> 40,
          "lng" -> 5
        )
      )
      val instance = Json.obj("_id" -> Json.obj(
        "$oid" -> "52eb6c66e4b08a001831aa9a"
      ),
        "loc" -> Json.obj(
          "type" -> "Point",
          "coordinates" -> Json.arr(40, 5)
        ))
      def fromGeoJson(jsObject: JsObject): JsObject = JsObject(
        jsObject.fields.map(fd =>
          if (fd._1 == "loc") {
            val coords = (fd._2 \ "coordinates").as[JsArray]
            fd._1 -> Json.obj(
              "lat" -> coords(0),
              "lng" -> coords(1)
            )
          } else {
            fd
          }
        )
      )
      val mongoTransformer = new MongoTransformer(schema, List(), List(fromGeoJson, fromMongoId))
      val res = mongoTransformer.fromMongoJson(instance)
      res.get must beEqualTo(expected)
    }
  }

}