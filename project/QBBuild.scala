import sbt._
import sbt.Keys._
import com.typesafe.sbteclipse.plugin.EclipsePlugin._
import bintray.Plugin._
import scoverage.ScoverageSbtPlugin
import sbtrelease.ReleasePlugin
import sbtrelease.ReleasePlugin._

object Version {
  val jsonZipper    = "1.2"
  val play          = "2.3.3"
  val openCsv       = "2.1"
  val reactiveMongo = "0.10.5.akka23-SNAPSHOT"
  val scalaz        = "7.0.6"
  val scalameter    = "0.6"
  val specs2        = "2.3.12"
}

object Library {

  val jsonZipper    = "com.mandubian"     %% "play-json-zipper"    % Version.jsonZipper
  val openCsv       = "net.sf.opencsv"    %  "opencsv"             % Version.openCsv
  val play          = "com.typesafe.play" %% "play"                % Version.play
  val playJson      = "com.typesafe.play" %% "play-json"           % Version.play
  val reactiveMongo = "org.reactivemongo" %% "play2-reactivemongo" % Version.reactiveMongo
  val scalaz        = "org.scalaz"        %% "scalaz-core"         % Version.scalaz
  val playTest      = "com.typesafe.play" %% "play-test"           % Version.play           % "test"
  val scalameter    = "com.storm-enroute" %% "scalameter"          % Version.scalameter     % "test"
  val specs2        = "org.specs2"        %% "specs2"              % Version.specs2         % "test"

}

object Dependencies {
  import Library._

  val qbSchema = List(
    playJson,
    jsonZipper,
    scalaz,
    scalameter,
    specs2
  )

  val qbPlay = List(
    play,
    playTest,
    jsonZipper,
    reactiveMongo,
    scalameter,
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


  val commonSettings = Seq(ScoverageSbtPlugin.instrumentSettings:_*) ++
    Seq(CoverallsPlugin.coverallsSettings:_*) ++
    Seq(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*) ++
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
    .aggregate(schemaProject, playProject, csvProject)

  lazy val schemaProject = Project("qbschema", file("qbschema"))
    .settings(buildSettings: _*)
    .settings(releaseSettings: _*)
    .settings(
      resolvers ++= QBRepositories,
      retrieveManaged := true,
      libraryDependencies ++= Dependencies.qbSchema,
      testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework")
    )

  lazy val playProject = Project("qbplay", file("qbplay"))
    .settings(buildSettings: _*)
    .settings(releaseSettings: _*)
    .settings(
      resolvers ++= QBRepositories,
      retrieveManaged := true,
      libraryDependencies ++= Dependencies.qbPlay,
      testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework")
    ).dependsOn(schemaProject)

  lazy val csvProject = Project("qbcsv", file("qbcsv"))
    .settings(buildSettings: _*)
    .settings(releaseSettings: _*)
    .settings(
      resolvers ++= QBRepositories,
      retrieveManaged := true,
      libraryDependencies ++= Dependencies.qbCsv
    ).dependsOn(schemaProject)

  lazy val qbForms = Project("qbforms", file("qbforms"))
    .enablePlugins(play.PlayScala)
    .settings(commonSettings: _*)
    .settings(releaseSettings: _*)
    .settings(
      resolvers ++= QBRepositories,
      retrieveManaged := true,
      libraryDependencies ++= Dependencies.qbForms
    ).dependsOn(schemaProject, playProject)

  lazy val playSampleProject =  Project("qbplay-sample", file("qbplay-sample"))
    .enablePlugins(play.PlayScala)
    .settings(releaseSettings: _*)
    .settings(commonSettings: _*)
    .settings(
      resolvers ++= QBRepositories,
      libraryDependencies ++= Dependencies.sample
    ).dependsOn(schemaProject,playProject,qbForms)
    .aggregate(schemaProject,playProject,qbForms)




}