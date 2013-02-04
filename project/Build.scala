import sbt._
import Keys._

object SummingbirdBuild extends Build {
  val sharedSettings = Project.defaultSettings ++ Seq(
    organization := "com.twitter",
    version := "0.0.1",
    scalaVersion := "2.9.2",
    libraryDependencies ++= Seq(
      "org.scalacheck" %% "scalacheck" % "1.10.0" % "test",
      "org.scala-tools.testing" % "specs_2.9.1" % "1.6.9" % "test"
    ),

    resolvers ++= Seq(
      "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
      "releases"  at "http://oss.sonatype.org/content/repositories/releases",
      "Clojars Repository" at "http://clojars.org/repo",
      "Conjars Repository" at "http://conjars.org/repo",
      "Twitter Artifactory" at "http://artifactory.local.twitter.com/repo"
    ),

    parallelExecution in Test := true,

    scalacOptions ++= Seq("-unchecked", "-deprecation"),

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
          <id>oscar</id>
          <name>Ashutosh Singhal</name>
          <url>http://twitter.com/daashu</url>
        </developer>
      </developers>)
  )

  val bijectionVersion = "0.2.1"
  val storehausVersion = "0.0.1"

  lazy val summingbird = Project(
    id = "summingbird",
    base = file(".")
    ).settings(
    test := { }
  ).aggregate(summingbirdCore,
              summingbirdBatch,
              summingbirdClient)

  lazy val summingbirdBatch = Project(
    id = "summingbird-batch",
    base = file("summingbird-batch"),
    settings = sharedSettings
  ).settings(
    name := "summingbird-batch",
    libraryDependencies ++= Seq(
      "com.twitter" %% "algebird" % "0.1.7",
      "com.twitter" %% "bijection-core" % bijectionVersion,
      "com.twitter" % "util-core" % "5.3.15"
    )
  )

  lazy val summingbirdClient = Project(
    id = "summingbird-client",
    base = file("summingbird-client"),
    settings = sharedSettings
  ).settings(
    name := "summingbird-client",
    libraryDependencies ++= Seq(
      "com.twitter" %% "algebird" % "0.1.7",
      "com.twitter" %% "bijection-core" % bijectionVersion,
      "com.twitter" %% "storehaus-core" % storehausVersion
    )
  ).dependsOn(summingbirdBatch)

  lazy val summingbirdCore = Project(
    id = "summingbird-core",
    base = file("summingbird-core"),
    settings = sharedSettings
  ).settings(
    name := "summingbird-core",
    libraryDependencies ++= Seq(
      "backtype" % "dfs-datastores" % "1.2.0",
      "com.twitter" %% "algebird" % "0.1.7",
      "com.twitter" %% "bijection-core" % bijectionVersion,
      "com.twitter" %% "bijection-json" % bijectionVersion,
      "com.twitter" %% "chill" % "0.1.3",
      "com.twitter" %% "scalding" % "0.8.2",
      "com.twitter" %% "scalding-commons" % "0.1.1",
      "com.twitter" %% "storehaus-core" % storehausVersion,
      "com.twitter" %% "tormenta" % "0.2.1",
      "com.twitter" % "util-core" % "5.3.15",
      "storm" % "storm" % "0.9.0-wip15",
      "storm" % "storm-kafka" % "0.9.0-wip6-scala292-multischeme",
      "storm" % "storm-kestrel" % "0.9.0-wip5-multischeme"
    )
  ).dependsOn(summingbirdBatch)
}
