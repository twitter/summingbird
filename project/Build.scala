package summingbird

import sbt._
import Keys._
import sbtgitflow.ReleasePlugin._

object SummingbirdBuild extends Build {
  val sharedSettings = Project.defaultSettings ++ releaseSettings ++ Seq(
    organization := "com.twitter",
    version := "0.0.2",
    scalaVersion := "2.9.2",
    libraryDependencies ++= Seq(
      "org.scalacheck" %% "scalacheck" % "1.10.0" % "test",
      "org.scala-tools.testing" %% "specs" % "1.6.9" % "test",
      "org.scalatest" %% "scalatest" % "1.6.1" % "test"
    ),

    resolvers ++= Seq(
      "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
      "releases"  at "http://oss.sonatype.org/content/repositories/releases",
      "Clojars Repository" at "http://clojars.org/repo",
      "Conjars Repository" at "http://conjars.org/repo",
      "Twitter Artifactory" at "http://artifactory.local.twitter.com/repo"
    ),

    parallelExecution in Test := true,

    scalacOptions ++= Seq(
      "-unchecked",
      "-deprecation",
      "-Yresolve-term-conflict:package"
    ),

    // Publishing options:
    publishMavenStyle := true,

    publishArtifact in Test := false,

    pomIncludeRepository := { x => false },

    publishTo <<= version { (v: String) =>
      val nexus = "http://artifactory.local.twitter.com/"
      if (v.trim.endsWith("SNAPSHOT"))
        Some("sonatype-snapshots" at nexus + "libs-snapshots-local")
      else
        Some("sonatype-releases"  at nexus + "libs-releases-local")
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
    test := { }
  ).aggregate(
    summingbirdCore,
    summingbirdBatch,
    summingbirdClient
  )

  val dfsDatastoresVersion = "1.3.4"
  val bijectionVersion = "0.4.0"
  val algebirdVersion = "0.1.13"
  val scaldingVersion = "0.8.5"
  val storehausVersion = "0.4.0"
  val utilVersion = "6.3.0"
  val chillVersion = "0.2.3"

  lazy val summingbirdBatch = Project(
    id = "summingbird-batch",
    base = file("summingbird-batch"),
    settings = sharedSettings
  ).settings(
    name := "summingbird-batch",
    libraryDependencies ++= Seq(
      "com.twitter" %% "algebird-core" % algebirdVersion,
      "com.twitter" %% "bijection-core" % bijectionVersion,
      "com.twitter" % "util-core" % utilVersion
    )
  )

  lazy val summingbirdClient = Project(
    id = "summingbird-client",
    base = file("summingbird-client"),
    settings = sharedSettings
  ).settings(
    name := "summingbird-client",
    libraryDependencies ++= Seq(
      "com.twitter" %% "algebird-core" % algebirdVersion,
      "com.twitter" %% "bijection-core" % bijectionVersion,
      "com.twitter" %% "storehaus-core" % storehausVersion,
      "com.twitter" %% "storehaus-algebra" % storehausVersion
    )
  ).dependsOn(summingbirdBatch)

  lazy val summingbirdCore = Project(
    id = "summingbird-core",
    base = file("summingbird-core"),
    settings = sharedSettings
  ).settings(
    name := "summingbird-core",
    libraryDependencies ++= Seq(
      "com.backtype" % "dfs-datastores" % dfsDatastoresVersion,
      "com.backtype" % "dfs-datastores-cascading" % dfsDatastoresVersion,
      "com.twitter" %% "algebird-core" % algebirdVersion,
      "com.twitter" %% "algebird-util" % algebirdVersion,
      "com.twitter" %% "bijection-core" % bijectionVersion,
      "com.twitter" %% "bijection-algebird" % bijectionVersion,
      "com.twitter" %% "bijection-json" % bijectionVersion,
      "com.twitter" %% "chill" % chillVersion,
      "com.twitter" %% "scalding-core" % scaldingVersion,
      "com.twitter" %% "scalding-commons" % "0.2.0",
      "com.twitter" %% "storehaus-core" % storehausVersion,
      "com.twitter" %% "storehaus-algebra" % storehausVersion,
      "com.twitter" %% "tormenta" % "0.4.0",
      "com.twitter" % "util-core" % utilVersion,
      "storm" % "storm" % "0.9.0-wip15",
      "storm" % "storm-kafka" % "0.9.0-wip6-scala292-multischeme",
      "storm" % "storm-kestrel" % "0.9.0-wip5-multischeme"
    )
  ).dependsOn(summingbirdBatch)
}
