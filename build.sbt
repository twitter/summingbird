import AssemblyKeys._
import ReleaseTransformations._
import com.typesafe.sbt.SbtScalariform._
import com.typesafe.tools.mima.plugin.MimaPlugin.mimaDefaultSettings
import sbtassembly.Plugin._
import scalariform.formatter.preferences._
import summingbird._

def scalaBinaryVersion(scalaVersion: String) = scalaVersion match {
  case version if version startsWith "2.10" => "2.10"
  case version if version startsWith "2.11" => "2.11"
  case version if version startsWith "2.12" => "2.12"
  case _ => sys.error("Unsupported scala version: " + scalaVersion)
}

def isScala210x(scalaVersion: String) = scalaBinaryVersion(scalaVersion) == "2.10"

val algebirdVersion = "0.12.0"
val bijectionVersion = "0.9.1"
val chillVersion = "0.7.3"
val commonsHttpClientVersion = "3.1"
val commonsLangVersion = "2.6"
val dagonVersion = "0.3.0"
val hadoopVersion = "1.2.1"
val junitVersion = "4.11"
val log4jVersion = "1.2.16"
val novocodeJunitVersion = "0.10"
val scalaCheckVersion = "1.12.2"
val scalatestVersion = "2.2.4"
val scaldingVersion = "0.16.0-RC3"
val slf4jVersion = "1.6.6"
val storehausVersion = "0.13.0"
val stormDep = "storm" % "storm" % "0.9.0-wip15" //This project also compiles with the latest storm, which is in fact required to run the example
val tormentaVersion = "0.11.1"
val utilVersion = "6.26.0"

val extraSettings = Project.defaultSettings ++ mimaDefaultSettings ++ scalariformSettings

val sharedSettings = extraSettings ++ Seq(
  organization := "com.twitter",
  scalaVersion := "2.11.11",
  crossScalaVersions := Seq("2.10.7", "2.11.11"),
  // To support hadoop 1.x
  javacOptions ++= Seq("-source", "1.6", "-target", "1.6"),

  javacOptions in doc ~= { (options: Seq[String]) =>
    val targetPos = options.indexOf("-target")
    if(targetPos > -1) {
      options.take(targetPos) ++ options.drop(targetPos + 2)
    } else options
  },

  libraryDependencies ++= Seq(
    "junit" % "junit" % junitVersion % "test",
    "org.slf4j" % "slf4j-api" % slf4jVersion,
    "org.scalacheck" %% "scalacheck" % scalaCheckVersion % "test",
    // These satisify's scaldings log4j needs when in test mode
    "log4j" % "log4j" % log4jVersion % "test",
    "com.novocode" % "junit-interface" % novocodeJunitVersion % "test",
    "org.scalatest" %% "scalatest" % scalatestVersion % "test"
  ),

  resolvers ++= Seq(
    Opts.resolver.sonatypeSnapshots,
    Opts.resolver.sonatypeReleases,
    "Clojars Repository" at "http://clojars.org/repo",
    "Conjars Repository" at "http://conjars.org/repo",
    "Twitter Maven" at "https://maven.twttr.com"
  ),

  parallelExecution in Test := true,

  scalacOptions ++= Seq(
    "-unchecked",
    "-deprecation",
    "-Xlint",
    "-Yresolve-term-conflict:package"
  ),

  // Publishing options:
  releaseCrossBuild := true,
  releasePublishArtifactsAction := PgpKeys.publishSigned.value,
  releaseVersionBump := sbtrelease.Version.Bump.Minor, // need to tweak based on mima results
  publishMavenStyle := true,
  publishArtifact in Test := false,
  pomIncludeRepository := { x => false },

  releaseProcess := Seq[ReleaseStep](
    checkSnapshotDependencies,
    inquireVersions,
    runClean,
//    runTest, // these tests are monsters and a bit flakey. Make sure to publish only green builds!
    setReleaseVersion,
    commitReleaseVersion,
    tagRelease,
    publishArtifacts,
    setNextVersion,
    commitNextVersion,
    ReleaseStep(action = Command.process("sonatypeReleaseAll", _)),
    pushChanges),

  publishTo := {
    val v = version.value
    Some(
      if (v.trim.toUpperCase.endsWith("SNAPSHOT"))
        Opts.resolver.sonatypeSnapshots
      else
        Opts.resolver.sonatypeStaging
        //"twttr" at "http://artifactory.local.twitter.com/libs-releases-local"
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

lazy val formattingPreferences = {
 import scalariform.formatter.preferences._
 FormattingPreferences().
   setPreference(AlignParameters, false).
   setPreference(PreserveSpaceBeforeArguments, true)
}

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
  summingbirdBatchHadoop,
  summingbirdOnline,
  summingbirdClient,
  summingbirdStorm,
  summingbirdStormTest,
  summingbirdScalding,
  summingbirdScaldingTest,
  summingbirdBuilder,
  summingbirdChill,
  summingbirdExample,
  summingbirdCoreTest
)

/**
  * This returns the previous jar we released that is compatible with
  * the current.
  */
val unreleasedModules = Set[String]()

def previousCompatible(subProj: String) =
  Set(subProj)
    .filterNot(unreleasedModules.contains(_))
    .map { s => "com.twitter" %% ("summingbird-" + s) % "0.10.0-RC2" }

def module(name: String) = {
  val id = "summingbird-" + name
  Project(id = id, base = file(id), settings = sharedSettings ++ Seq(
    Keys.name := id,
    mimaPreviousArtifacts := previousCompatible(name),
    mimaBinaryIssueFilters ++= {
      import com.typesafe.tools.mima.core._
      import com.typesafe.tools.mima.core.ProblemFilters._
      Seq(
        // this class should only be used internally, so deleting between RC2 -> final should be
        // okay
        exclude[DirectMissingMethodProblem]("com.twitter.summingbird.scalding.Memo.getOrElseUpdate")
      )
    })
  )
}

lazy val summingbirdBatch = module("batch").settings(
  libraryDependencies ++= Seq(
    "com.twitter" %% "algebird-core" % algebirdVersion,
    "com.twitter" %% "bijection-core" % bijectionVersion,
    "com.twitter" %% "scalding-date" % scaldingVersion,
    "org.slf4j" % "slf4j-log4j12" % slf4jVersion % "test"
  )
)

lazy val summingbirdChill = module("chill").settings(
  libraryDependencies ++= Seq(
    "com.twitter" %% "chill" % chillVersion,
    "com.twitter" %% "chill-bijection" % chillVersion,
    "org.slf4j" % "slf4j-log4j12" % slf4jVersion % "test"
  )
).dependsOn(
    summingbirdCore,
    summingbirdBatch
)

lazy val summingbirdClient = module("client").settings(
  libraryDependencies ++= Seq(
    "com.twitter" %% "algebird-core" % algebirdVersion,
    "com.twitter" %% "algebird-util" % algebirdVersion,
    "com.twitter" %% "bijection-core" % bijectionVersion,
    "com.twitter" %% "storehaus-core" % storehausVersion,
    "com.twitter" %% "storehaus-algebra" % storehausVersion,
    "org.slf4j" % "slf4j-log4j12" % slf4jVersion % "test"
  )
).dependsOn(summingbirdBatch)

lazy val summingbirdCore = module("core").settings(
  libraryDependencies ++= Seq(
    "com.twitter" %% "algebird-core" % algebirdVersion,
    "com.stripe" %% "dagon-core" % dagonVersion)
)

lazy val summingbirdOnline = module("online").settings(
  libraryDependencies ++= Seq(
    "com.twitter" %% "algebird-core" % algebirdVersion,
    "com.twitter" %% "algebird-util" % algebirdVersion,
    "com.twitter" %% "bijection-core" % bijectionVersion,
    "com.twitter" %% "storehaus-core" % storehausVersion,
    "com.twitter" %% "chill" % chillVersion,
    "com.twitter" %% "storehaus-algebra" % storehausVersion,
    "com.twitter" %% "util-core" % utilVersion,
    "org.slf4j" % "slf4j-log4j12" % slf4jVersion % "test"
  )
).dependsOn(
  summingbirdCore % "test->test;compile->compile",
  summingbirdCoreTest % "test->test",
  summingbirdBatch,
  summingbirdClient
)

lazy val summingbirdStorm = module("storm").settings(
  parallelExecution in Test := false,
  libraryDependencies ++= Seq(
    "com.twitter" %% "algebird-core" % algebirdVersion,
    "com.twitter" %% "bijection-core" % bijectionVersion,
    "com.twitter" %% "chill" % chillVersion,
    "com.twitter" % "chill-storm" % chillVersion,
    "com.twitter" %% "chill-bijection" % chillVersion,
    "com.twitter" %% "storehaus-core" % storehausVersion,
    "com.twitter" %% "storehaus-algebra" % storehausVersion,
    "com.twitter" %% "scalding-args" % scaldingVersion,
    "com.twitter" %% "tormenta-core" % tormentaVersion,
    "com.twitter" %% "util-core" % utilVersion,
    stormDep % "provided"
  )
).dependsOn(
  summingbirdCore % "test->test;compile->compile",
  summingbirdOnline,
  summingbirdChill,
  summingbirdBatch
)

lazy val summingbirdStormTest = module("storm-test").settings(
  parallelExecution in Test := false,
  libraryDependencies ++= Seq(
    "com.twitter" %% "algebird-core" % algebirdVersion,
    "com.twitter" %% "bijection-core" % bijectionVersion,
    "com.twitter" %% "storehaus-core" % storehausVersion,
    "com.twitter" %% "storehaus-algebra" % storehausVersion,
    "com.twitter" %% "tormenta-core" % tormentaVersion,
    "com.twitter" %% "util-core" % utilVersion,
    stormDep % "provided"
  )
).dependsOn(
  summingbirdCore % "test->test;compile->compile",
  summingbirdCoreTest % "test->test",
  summingbirdStorm
)

lazy val summingbirdScalding = module("scalding").settings(
  libraryDependencies ++= Seq(
    "com.twitter" %% "algebird-core" % algebirdVersion,
    "com.twitter" %% "algebird-util" % algebirdVersion,
    "com.twitter" %% "algebird-bijection" % algebirdVersion,
    "com.twitter" %% "bijection-json" % bijectionVersion,
    "com.twitter" %% "chill" % chillVersion,
    "com.twitter" % "chill-hadoop" % chillVersion,
    "com.twitter" %% "chill-bijection" % chillVersion,
    "commons-lang" % "commons-lang" % commonsLangVersion,
    "com.twitter" %% "scalding-core" % scaldingVersion,
    "com.twitter" %% "scalding-commons" % scaldingVersion,
    "org.apache.hadoop" % "hadoop-core" % hadoopVersion % "provided"
  )
).dependsOn(
  summingbirdCore % "test->test;compile->compile",
  summingbirdChill,
  summingbirdBatchHadoop,
  summingbirdBatch
)

lazy val summingbirdScaldingTest = module("scalding-test").settings(
  libraryDependencies ++= Seq(
    "org.scalacheck" %% "scalacheck" % scalaCheckVersion,
    "org.slf4j" % "slf4j-log4j12" % slf4jVersion % "test",
    "org.apache.hadoop" % "hadoop-core" % hadoopVersion % "provided"
  )
).dependsOn(
  summingbirdCore % "test->test;compile->compile",
  summingbirdCoreTest % "test->test",
  summingbirdChill,
  summingbirdBatchHadoop,
  summingbirdScalding
)

lazy val summingbirdBatchHadoop = module("batch-hadoop").settings(
  libraryDependencies ++= Seq(
    "com.twitter" %% "algebird-core" % algebirdVersion,
    "com.twitter" %% "bijection-json" % bijectionVersion,
    "com.twitter" %% "scalding-commons" % scaldingVersion,
    "com.twitter" %% "scalding-date" % scaldingVersion,
    "org.slf4j" % "slf4j-log4j12" % slf4jVersion % "test",
    "org.apache.hadoop" % "hadoop-core" % hadoopVersion % "provided"
  )
).dependsOn(
  summingbirdCore % "test->test;compile->compile",
  summingbirdBatch
)

lazy val summingbirdBuilder = module("builder").settings(
  libraryDependencies ++= Seq(
    stormDep % "provided",
    "org.apache.hadoop" % "hadoop-core" % hadoopVersion % "provided"
  )
).dependsOn(
  summingbirdCore,
  summingbirdStorm,
  summingbirdScalding
)

lazy val summingbirdExample = module("example").settings(
  libraryDependencies ++= Seq(
    "log4j" % "log4j" % log4jVersion,
    "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
    stormDep exclude("org.slf4j", "log4j-over-slf4j") exclude("ch.qos.logback", "logback-classic"),
    "com.twitter" %% "bijection-netty" % bijectionVersion,
    "com.twitter" %% "tormenta-twitter" % tormentaVersion,
    "com.twitter" %% "storehaus-memcache" % storehausVersion exclude("com.twitter.common", "dynamic-host-set") exclude("com.twitter.common", "service-thrift"),
    "org.slf4j" % "slf4j-log4j12" % slf4jVersion % "test"
  )
).dependsOn(summingbirdCore, summingbirdStorm)

lazy val summingbirdCoreTest = module("core-test").settings(
  parallelExecution in Test := false,
  libraryDependencies ++=Seq(
    "junit" % "junit" % junitVersion % "provided",
    "org.slf4j" % "slf4j-api" % slf4jVersion % "provided",
    "org.scalacheck" %% "scalacheck" % scalaCheckVersion % "provided",
    "org.scalatest" %% "scalatest" % scalatestVersion % "provided")

).dependsOn(
    summingbirdCore % "test->test;compile->compile"
  )
