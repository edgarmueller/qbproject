import sbt._
import sbt.Keys._
import play.Project._
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
    "Mandubian repository releases" at "https://github.com/mandubian/mandubian-mvn/tree/master/releases"
  )

  val buildSettings = Project.defaultSettings ++
    Seq(ScoverageSbtPlugin.instrumentSettings:_*) ++
    Seq(CoverallsPlugin.coverallsSettings:_*) ++
    Seq(
      organization := "org.qbproject",
      scalaVersion := "2.10.4",
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
        "com.typesafe.play" %% "play-json"         % "2.2.3",
        "com.mandubian"     %% "play-json-zipper"  % "1.1",
        "org.specs2"        %% "specs2"            % "2.3.7"  % "test",
        "org.scalaz"        %% "scalaz-core"       % "7.0.5",
        "com.github.axel22" %% "scalameter"        % "0.4"      % "test"
      ),
      testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework")
    )

  lazy val playProject = Project("qbplay", file("qbplay"))
    .settings(buildSettings: _*)
    .settings(releaseSettings: _*)
    .settings(
      resolvers ++= QBRepositories,
      retrieveManaged := true,
      libraryDependencies ++= Seq(
        "com.typesafe.play" %% "play"                % "2.2.3",
        "com.typesafe.play" %% "play-test"           % "2.2.3"    % "test",
        "com.mandubian"     %% "play-json-zipper"    % "1.1",
        "org.reactivemongo" %% "play2-reactivemongo" % "0.10.2",
        "com.github.axel22" %% "scalameter"          % "0.4"      % "test",
        "org.specs2"        %% "specs2"              % "2.3.7"    % "test"
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
        "com.typesafe.play" %% "play-json"           % "2.2.3",
        "net.sf.opencsv"    %  "opencsv"             % "2.1",
        "org.specs2"        %% "specs2"              % "2.3.7"    % "test",
        "org.scalaz"        %% "scalaz-core"         % "7.0.5"
      )
    ).dependsOn(schemaProject)

  lazy val playSampleProject = play.Project("qbplay-sample", "ignore", path = file("qbplay-sample"))
    .settings(buildSettings: _*)
    .settings(releaseSettings: _*)
    .settings(playScalaSettings : _*)
    .settings(
      resolvers ++= QBRepositories,
      libraryDependencies ++= Seq(
        "org.specs2" %% "specs2"      % "1.13" % "test",
        "junit"      %  "junit"       % "4.8"  % "test",
        "org.scalaz" %% "scalaz-core" % "7.0.5"
      )
    )
    .dependsOn(schemaProject,playProject)
    .aggregate(schemaProject,playProject)

}
