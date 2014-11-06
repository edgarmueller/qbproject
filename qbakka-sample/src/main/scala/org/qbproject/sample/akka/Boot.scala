package org.qbproject.sample.akka

import akka.actor.{Props, ActorSystem}
import akka.io.IO
import spray.can.Http

object Boot extends App {

  // create an actor system for application
  implicit val system = ActorSystem("qbakka-sample")

  // create and start rest service actor
  val service = system.actorOf(Props[SampleServiceActor], "qbakka-sample-endpoint")

  // start a new HTTP server on port 8080 with our service actor as the handler
  IO(Http) ! Http.Bind(service, interface = "localhost", port = 9090)
}