import play.PlayImport.PlayKeys
import sbt._
import sbt.Keys._
import com.typesafe.sbteclipse.plugin.EclipsePlugin._
import bintray.Plugin._
import sbtrelease.ReleasePlugin
import sbtrelease.ReleasePlugin._

object Version {
  val play          = "2.4.2"
  val openCsv       = "2.1"
  val reactiveMongo = "0.10.5.0.akka23"
  val scalaz        = "7.0.6"
  val scalameter    = "0.6"
  val specs2        = "2.3.13"
  val spray         = "1.3.2"
  val akka          = "2.3.6"
  val scalaLogging  = "3.1.0"
}

object Library {

  val openCsv       = "net.sf.opencsv"    %  "opencsv"                % Version.openCsv
  val sprayCan      = "io.spray"          %% "spray-can"              % Version.spray
  val sprayHttp     = "io.spray"          %% "spray-http"             % Version.spray
  val sprayRouting  = "io.spray"          %% "spray-routing"          % Version.spray
  val reactiveMongo = "org.reactivemongo" %% "play2-reactivemongo"    % Version.reactiveMongo
  val scalaz        = "org.scalaz"        %% "scalaz-core"            % Version.scalaz
  val akkaActor     = "com.typesafe.akka" %% "akka-actor"             % Version.akka
  val play          = "com.typesafe.play" %% "play"                   % Version.play
  val playJson      = "com.typesafe.play" %% "play-json"              % Version.play
  val scalaLogging  = "com.typesafe.scala-logging" %% "scala-logging" % Version.scalaLogging
  val playTest      = "com.typesafe.play" %% "play-test"              % Version.play           % "test"
  val scalameter    = "com.storm-enroute" %% "scalameter"             % Version.scalameter     % "test"
  val specs2        = "org.specs2"        %% "specs2"                 % Version.specs2         % "test"

}

object Dependencies {
  import Library._

  val qbSchema = List(
    playJson,
    scalaz,
    scalameter,
    specs2
  )

  val qbPlay = List(
    play,
    playTest,
    scalameter,
    specs2
  )

  val qbMongo = List(
    playJson,
    reactiveMongo,
    scalaLogging,
    scalaz,
    specs2
  )

  val qbCsv = List(
    playJson,
    openCsv,
    scalaz,
    specs2
  )

  val qbForms = List(

  )

  val akkaSample = List(
    playJson,
    sprayCan,
    sprayHttp,
    sprayRouting,
    akkaActor,
    reactiveMongo
  )

  val sample = List(
    scalaz,
    specs2
  )
}

object QBBuild extends Build {

  val QBRepositories = Seq(
    "Typesafe repository"           at "http://repo.typesafe.com/typesafe/releases/",
    "mandubian maven bintray"       at "http://dl.bintray.com/mandubian/maven",
    "Sonatype OSS Snapshots"        at "https://oss.sonatype.org/content/repositories/snapshots",
    "Sonatype OSS Releases"         at "https://oss.sonatype.org/content/repositories/releases",
    "Mandubian repository releases" at "https://github.com/mandubian/mandubian-mvn/tree/master/releases"
  )


  val commonSettings = Seq(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*) ++
    Seq(
      organization := "org.qbproject",
      scalaVersion := "2.11.2",
      crossScalaVersions := Seq("2.10.4","2.11.2"),
      licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
      EclipseKeys.projectFlavor := EclipseProjectFlavor.Scala,
      EclipseKeys.skipParents in ThisBuild := false,
      EclipseKeys.executionEnvironment := Some(EclipseExecutionEnvironment.JavaSE16),
      EclipseKeys.withSource := true,
      Keys.fork in Test := false,
      Keys.parallelExecution in Test := false
    )

  // TODO: remove setting
  val buildSettings = Project.defaultSettings ++ commonSettings

  val releaseSettings = ReleasePlugin.releaseSettings ++ bintrayPublishSettings ++ Seq(
      publishMavenStyle := true,
      publishTo := (publishTo in bintray.Keys.bintray).value, // set it globally so that sbt-release plugin does not freak out.
      bintray.Keys.bintrayOrganization in bintray.Keys.bintray := Some("qbproject"),
      ReleaseKeys.crossBuild := true
  )

  lazy val root = Project("qbroot", file("."))
    .settings(buildSettings: _*)
    .settings(releaseSettings: _*)
    .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
    .settings(
      unmanagedSourceDirectories in Compile <+= baseDirectory(new File(_, "src/main/scala")),
      unmanagedSourceDirectories in Test    <+= baseDirectory(new File(_, "src/test/scala")),
      retrieveManaged := true
    )
    .aggregate(schemaProject)

  lazy val schemaProject = Project("qbschema", file("qbschema"))
    .settings(buildSettings: _*)
    .settings(releaseSettings: _*)
    .settings(
      resolvers ++= QBRepositories,
      retrieveManaged := true,
      libraryDependencies ++= Dependencies.qbSchema,
      testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework")
    )

}