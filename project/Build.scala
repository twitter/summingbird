package summingbird

import sbt._
import Keys._
import sbtgitflow.ReleasePlugin._
import com.typesafe.tools.mima.plugin.MimaPlugin.mimaDefaultSettings
import com.typesafe.tools.mima.plugin.MimaKeys.previousArtifact

object SummingbirdBuild extends Build {
  def withCross(dep: ModuleID) =
    dep cross CrossVersion.binaryMapped {
      case "2.9.3" => "2.9.2" // TODO: hack because twitter hasn't built things against 2.9.3
      case version if version startsWith "2.10" => "2.10" // TODO: hack because sbt is broken
      case x => x
    }

  val sharedSettings = Project.defaultSettings ++ releaseSettings ++ Seq(
    organization := "com.twitter",
    version := "0.2.0-SNAPSHOT",
    scalaVersion := "2.9.3",
    crossScalaVersions := Seq("2.9.3", "2.10.0"),
    libraryDependencies ++= Seq(
      "org.scalacheck" %% "scalacheck" % "1.10.0" % "test",
      "org.scala-tools.testing" %% "specs" % "1.6.9" % "test",
      // These satisify's scaldings log4j needs when in test mode
      "log4j" % "log4j" % "1.2.16" % "test",
      "org.slf4j" % "slf4j-log4j12" % "1.6.6" % "test"
    ),

    resolvers ++= Seq(
      Opts.resolver.sonatypeSnapshots,
      Opts.resolver.sonatypeReleases,
      "Clojars Repository" at "http://clojars.org/repo",
      "Conjars Repository" at "http://conjars.org/repo",
      "Twitter Maven" at "http://maven.twttr.com"
    ),

    parallelExecution in Test := false, // until scalding 0.9.0 we can't do this

    scalacOptions ++= Seq(
      "-unchecked",
      "-deprecation",
      "-Yresolve-term-conflict:package"
    ),

    // Publishing options:
    publishMavenStyle := true,

    publishArtifact in Test := false,

    pomIncludeRepository := { x => false },

    publishTo <<= version { v =>
      Some(
        if (v.trim.toUpperCase.endsWith("SNAPSHOT"))
          Opts.resolver.sonatypeSnapshots
        else
          Opts.resolver.sonatypeStaging
      )
    },

    pomExtra := (
      <url>https://github.com/twitter/summingbird</url>
      <licenses>
        <license>
          <name>Apache 2</name>
          <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
          <distribution>repo</distribution>
          <comments>A business-friendly OSS license</comments>
        </license>
      </licenses>
      <scm>
        <url>git@github.com:twitter/summingbird.git</url>
        <connection>scm:git:git@github.com:twitter/summingbird.git</connection>
      </scm>
      <developers>
        <developer>
          <id>oscar</id>
          <name>Oscar Boykin</name>
          <url>http://twitter.com/posco</url>
        </developer>
        <developer>
          <id>sritchie</id>
          <name>Sam Ritchie</name>
          <url>http://twitter.com/sritchie</url>
        </developer>
        <developer>
          <id>asinghal</id>
          <name>Ashutosh Singhal</name>
          <url>http://twitter.com/daashu</url>
        </developer>
      </developers>)
  )

  lazy val summingbird = Project(
    id = "summingbird",
    base = file("."),
    settings = sharedSettings ++ DocGen.publishSettings
  ).settings(
    test := { },
    publish := { }, // skip publishing for this root project.
    publishLocal := { }
  ).aggregate(
    summingbirdCore,
    summingbirdBatch,
    summingbirdClient,
    summingbirdStorm,
    summingbirdScalding,
    summingbirdBuilder,
    summingbirdExample
  )

  val dfsDatastoresVersion = "1.3.4"
  val bijectionVersion = "0.5.4"
  val algebirdVersion = "0.3.0"
  val scaldingVersion = "0.9.0rc1"
  val storehausVersion = "0.6.0"
  val utilVersion = "6.3.8"
  val chillVersion = "0.3.3"
  val tormentaVersion = "0.5.3"

  /**
    * This returns the youngest jar we released that is compatible with
    * the current.
    */
  val unreleasedModules = Set[String]()

  def youngestForwardCompatible(subProj: String) =
    Some(subProj)
      .filterNot(unreleasedModules.contains(_))
      .map { s => "com.twitter" % ("summingbird-" + s + "_2.9.3") % "0.1.0" }

  def module(name: String) = {
    val id = "summingbird-%s".format(name)
    Project(id = id, base = file(id), settings = sharedSettings ++ Seq(
      Keys.name := id,
      previousArtifact := youngestForwardCompatible(name))
    )
  }

  lazy val summingbirdBatch = module("batch").settings(
    libraryDependencies ++= Seq(
      "com.twitter" %% "algebird-core" % algebirdVersion,
      "com.twitter" %% "bijection-core" % bijectionVersion
    )
  )

  lazy val summingbirdClient = module("client").settings(
    libraryDependencies ++= Seq(
      "com.twitter" %% "algebird-core" % algebirdVersion,
      "com.twitter" %% "algebird-util" % algebirdVersion,
      "com.twitter" %% "bijection-core" % bijectionVersion,
      "com.twitter" %% "storehaus-core" % storehausVersion,
      "com.twitter" %% "storehaus-algebra" % storehausVersion
    )
  ).dependsOn(summingbirdBatch)

  lazy val summingbirdCore = module("core").settings(
    libraryDependencies += "com.twitter" %% "algebird-core" % algebirdVersion
  )

  lazy val summingbirdStorm = module("storm").settings(
    parallelExecution in Test := false,
    libraryDependencies ++= Seq(
      "com.twitter" %% "algebird-core" % algebirdVersion,
      "com.twitter" %% "bijection-core" % bijectionVersion,
      "com.twitter" %% "chill" % chillVersion,
      "com.twitter" % "chill-storm" % chillVersion exclude("org.slf4j", "log4j-over-slf4j"),
      "com.twitter" %% "chill-bijection" % chillVersion,
      "com.twitter" %% "storehaus-core" % storehausVersion,
      "com.twitter" %% "storehaus-algebra" % storehausVersion,
      "com.twitter" %% "tormenta-core" % tormentaVersion exclude("org.slf4j", "log4j-over-slf4j"),
      withCross("com.twitter" %% "util-core" % utilVersion),
      "storm" % "storm" % "0.9.0-wip15" % "provided" exclude("org.slf4j", "log4j-over-slf4j")
    )
  ).dependsOn(
    summingbirdCore % "test->test;compile->compile",
    summingbirdBatch
  )

  lazy val summingbirdScalding = module("scalding").settings(
    libraryDependencies ++= Seq(
      "com.backtype" % "dfs-datastores" % dfsDatastoresVersion,
      "com.backtype" % "dfs-datastores-cascading" % dfsDatastoresVersion,
      "com.twitter" %% "algebird-core" % algebirdVersion,
      "com.twitter" %% "algebird-util" % algebirdVersion,
      "com.twitter" %% "algebird-bijection" % algebirdVersion,
      "com.twitter" %% "bijection-json" % bijectionVersion,
      "com.twitter" %% "chill" % chillVersion,
      "com.twitter" % "chill-hadoop" % chillVersion,
      "com.twitter" %% "chill-bijection" % chillVersion,
      "commons-lang" % "commons-lang" % "2.6",
      "com.twitter" %% "scalding-core" % scaldingVersion,
      "com.twitter" %% "scalding-commons" % scaldingVersion
    )
  ).dependsOn(
    summingbirdCore % "test->test;compile->compile",
    summingbirdBatch
  )

  lazy val summingbirdBuilder = module("builder").dependsOn(
    summingbirdCore,
    summingbirdStorm,
    summingbirdScalding
  )

  lazy val summingbirdExample = module("example").settings(
    libraryDependencies ++= Seq(
      "com.twitter" %% "bijection-netty" % bijectionVersion,
      "com.twitter" %% "tormenta-twitter" % tormentaVersion exclude("org.slf4j", "log4j-over-slf4j"),
      "com.twitter" %% "storehaus-memcache" % storehausVersion
    )
  ).dependsOn(summingbirdCore, summingbirdStorm)
}
