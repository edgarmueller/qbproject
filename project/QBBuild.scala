import sbt._
import sbt.Keys._
import com.typesafe.sbteclipse.plugin.EclipsePlugin._
import bintray.Plugin._
import scoverage.ScoverageSbtPlugin
import sbtrelease.ReleasePlugin
import sbtrelease.ReleasePlugin._

object QBBuild extends Build {

  val QBRepositories = Seq(
    "Typesafe repository"     at "http://repo.typesafe.com/typesafe/releases/",
    "mandubian maven bintray" at "http://dl.bintray.com/mandubian/maven",
    "Sonatype OSS Snapshots"  at "https://oss.sonatype.org/content/repositories/snapshots",
    "Sonatype OSS Releases"  at "https://oss.sonatype.org/content/repositories/releases",
    "Mandubian repository releases" at "https://github.com/mandubian/mandubian-mvn/tree/master/releases"
  )

  val buildSettings = Project.defaultSettings ++
    Seq(ScoverageSbtPlugin.instrumentSettings:_*) ++
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

  val releaseSettings = ReleasePlugin.releaseSettings ++ bintrayPublishSettings ++ Seq(
      publishMavenStyle := true,
      publishTo := (publishTo in bintray.Keys.bintray).value, // set it globally so that sbt-release plugin does not freak out.
      bintray.Keys.bintrayOrganization in bintray.Keys.bintray := Some("qbproject")
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
      libraryDependencies ++= Seq(
        "com.typesafe.play" %% "play-json"         % playVersion,
        "com.mandubian"     %% "play-json-zipper"  % "1.2",
        scalaz,
        specs2
      )
    )

  lazy val playProject = Project("qbplay", file("qbplay"))
    .settings(buildSettings: _*)
    .settings(releaseSettings: _*)
    .settings(
      resolvers ++= QBRepositories,
      retrieveManaged := true,
      libraryDependencies ++= Seq(
        "com.typesafe.play" %% "play"                % playVersion,
        "com.typesafe.play" %% "play-test"           % playVersion % "test",
        "com.mandubian"     %% "play-json-zipper"    % "1.2",
        "org.reactivemongo" %% "play2-reactivemongo" % "0.10.5.akka23-SNAPSHOT",
        "com.storm-enroute" %% "scalameter"          % "0.6"      % "test",
        specs2
      ),
      testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework")
    ).dependsOn(schemaProject)

  lazy val csvProject = Project("qbcsv", file("qbcsv"))
    .settings(buildSettings: _*)
    .settings(releaseSettings: _*)
    .settings(
      resolvers ++= QBRepositories,
      retrieveManaged := true,
      libraryDependencies ++= Seq(
        "com.typesafe.play" %% "play-json"           % playVersion,
        "net.sf.opencsv"    %  "opencsv"             % "2.1",
        scalaz,
        specs2
      )
    ).dependsOn(schemaProject)

  lazy val playSampleProject = Project("qbplay-sample", file("qbplay-sample"))
    .enablePlugins(play.PlayScala)
    .settings(buildSettings: _*)
    .settings(releaseSettings: _*)
//    .settings(playScalaSettings : _*)
    .settings(
      resolvers ++= QBRepositories,
      libraryDependencies ++= Seq(
        "junit"      %  "junit"       % "4.8"  % "test",
        scalaz,
        specs2
      )
    )
    .dependsOn(schemaProject,playProject)
    .aggregate(schemaProject,playProject)

  val specs2 = "org.specs2" %% "specs2" % "2.3.12" % "test"

  val scalaz = "org.scalaz" %% "scalaz-core" % "7.0.6"

  val playVersion = "2.3.3"
}