import AssemblyKeys._
import ReleaseTransformations._
import com.typesafe.sbt.SbtScalariform._
import com.typesafe.tools.mima.plugin.MimaPlugin.mimaDefaultSettings
import sbtassembly.Plugin._
import scalariform.formatter.preferences._

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
val finagleVersion = "6.35.0"
val hadoopVersion = "1.2.1"
val junitVersion = "4.11"
val log4jVersion = "1.2.16"
val novocodeJunitVersion = "0.10"
val scalaCheckVersion = "1.12.2"
val scalatestVersion = "2.2.4"
val scaldingVersion = "0.16.1-RC3"
val slf4jVersion = "1.6.6"
val storehausVersion = "0.15.0-RC1"
val stormDep = "storm" % "storm" % "0.9.0-wip15" //This project also compiles with the latest storm, which is in fact required to run the example
val tormentaVersion = "0.11.1"
val utilVersion = "6.34.0"

val extraSettings = mimaDefaultSettings ++ scalariformSettings

val sharedSettings = extraSettings ++ Seq(
  organization := "com.twitter",
  scalaVersion := "2.11.7",
  crossScalaVersions := Seq("2.10.5", "2.11.7"),
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
    "Twitter Maven" at "http://maven.twttr.com"
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

  publishTo := Some(
      if (version.value.trim.toUpperCase.endsWith("SNAPSHOT"))
        Opts.resolver.sonatypeSnapshots
      else
        Opts.resolver.sonatypeStaging
    ),

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

lazy val noPublishSettings = Seq(
    publish := (),
    publishLocal := (),
    test := (),
    publishArtifact := false
  )

lazy val summingbird = Project(
  id = "summingbird",
  base = file("."),
  settings = sharedSettings)
  .settings(noPublishSettings)
  .aggregate(
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
  * This returns the youngest jar we released that is compatible with
  * the current.
  */
val unreleasedModules = Set[String]()

def youngestForwardCompatible(subProj: String) =
  Some(subProj)
    .filterNot(unreleasedModules.contains(_))
    .map { s => "com.twitter" % ("summingbird-" + s + "_2.10") % "0.9.0" }

/**
  * Empty this each time we publish a new version (and bump the minor number)
  */
val ignoredABIProblems = {
  import com.typesafe.tools.mima.core._
  import com.typesafe.tools.mima.core.ProblemFilters._
  Seq(
    exclude[DirectMissingMethodProblem]("com.twitter.summingbird.memory.Memory.toStream"),
    exclude[ReversedMissingMethodProblem]("com.twitter.summingbird.planner.DagOptimizer.FlatMapValuesFusion"),
    exclude[ReversedMissingMethodProblem]("com.twitter.summingbird.planner.DagOptimizer.FlatMapKeyFusion"),
    exclude[IncompatibleResultTypeProblem]("com.twitter.summingbird.batch.store.HDFSMetadata.versionedStore"),
    exclude[DirectMissingMethodProblem]("com.twitter.summingbird.planner.OnlinePlan.this"),
    exclude[DirectMissingMethodProblem]("com.twitter.summingbird.planner.StripNamedNode.castTail"),
    exclude[DirectMissingMethodProblem]("com.twitter.summingbird.planner.StripNamedNode.functionize"),
    exclude[DirectMissingMethodProblem]("com.twitter.summingbird.planner.StripNamedNode.castToPair"),
    exclude[DirectMissingMethodProblem]("com.twitter.summingbird.planner.StripNamedNode.processLevel"),
    exclude[DirectMissingMethodProblem]("com.twitter.summingbird.planner.StripNamedNode.toFunctional"),
    exclude[DirectMissingMethodProblem]("com.twitter.summingbird.planner.StripNamedNode.mutateGraph"),
    exclude[DirectMissingMethodProblem]("com.twitter.summingbird.planner.StripNamedNode.stripNamedNodes"),
    exclude[ReversedMissingMethodProblem]("com.twitter.summingbird.online.OnlineDefaultConstants.com$twitter$summingbird$online$OnlineDefaultConstants$_setter_$DEFAULT_FM_MERGEABLE_WITH_SOURCE_="),
    exclude[ReversedMissingMethodProblem]("com.twitter.summingbird.online.OnlineDefaultConstants.DEFAULT_FM_MERGEABLE_WITH_SOURCE"),
    exclude[MissingClassProblem]("com.twitter.summingbird.online.MergeableStoreFactoryAlgebra"),
    exclude[MissingClassProblem]("com.twitter.summingbird.online.MergeableStoreFactoryAlgebra$"),
    exclude[IncompatibleResultTypeProblem]("com.twitter.summingbird.online.MergeableStoreFactory.mergeableStore"),
    exclude[ReversedMissingMethodProblem]("com.twitter.summingbird.online.MergeableStoreFactory.mergeableStore"),
    exclude[DirectMissingMethodProblem]("com.twitter.summingbird.online.executor.OperationContainer.init"),
    exclude[DirectMissingMethodProblem]("com.twitter.summingbird.online.executor.OperationContainer.decoder"),
    exclude[DirectMissingMethodProblem]("com.twitter.summingbird.online.executor.OperationContainer.encoder"),
    exclude[ReversedMissingMethodProblem]("com.twitter.summingbird.online.executor.OperationContainer.init"),
    exclude[DirectMissingMethodProblem]("com.twitter.summingbird.online.executor.FinalFlatMap.decoder"),
    exclude[DirectMissingMethodProblem]("com.twitter.summingbird.online.executor.FinalFlatMap.encoder"),
    exclude[DirectMissingMethodProblem]("com.twitter.summingbird.online.executor.FinalFlatMap.this"),
    exclude[DirectMissingMethodProblem]("com.twitter.summingbird.online.executor.IntermediateFlatMap.decoder"),
    exclude[DirectMissingMethodProblem]("com.twitter.summingbird.online.executor.IntermediateFlatMap.encoder"),
    exclude[DirectMissingMethodProblem]("com.twitter.summingbird.online.executor.IntermediateFlatMap.this"),
    exclude[DirectMissingMethodProblem]("com.twitter.summingbird.online.executor.AsyncBase.init"),
    exclude[IncompatibleResultTypeProblem]("com.twitter.summingbird.online.executor.AsyncBase.execute"),
    exclude[IncompatibleResultTypeProblem]("com.twitter.summingbird.online.executor.AsyncBase.executeTick"),
    exclude[DirectMissingMethodProblem]("com.twitter.summingbird.online.executor.AsyncBase.logger"),
    exclude[DirectMissingMethodProblem]("com.twitter.summingbird.online.executor.Summer.init"),
    exclude[DirectMissingMethodProblem]("com.twitter.summingbird.online.executor.Summer.this"),
    exclude[DirectMissingMethodProblem]("com.twitter.summingbird.online.executor.Summer.decoder"),
    exclude[DirectMissingMethodProblem]("com.twitter.summingbird.online.executor.Summer.encoder"),
    exclude[MissingClassProblem]("com.twitter.summingbird.scalding.LookupJoin"),
    exclude[MissingClassProblem]("com.twitter.summingbird.scalding.LookupJoin$"),
    exclude[IncompatibleResultTypeProblem]("com.twitter.summingbird.storm.BaseBolt.copy$default$8"),
    exclude[DirectMissingMethodProblem]("com.twitter.summingbird.storm.BaseBolt.copy"),
    exclude[DirectMissingMethodProblem]("com.twitter.summingbird.storm.BaseBolt.this"),
    exclude[IncompatibleMethTypeProblem]("com.twitter.summingbird.storm.Storm.get"),
    exclude[IncompatibleMethTypeProblem]("com.twitter.summingbird.storm.Storm.getOrElse"),
    exclude[DirectMissingMethodProblem]("com.twitter.summingbird.storm.BaseBolt.apply"),
    exclude[IncompatibleResultTypeProblem]("com.twitter.summingbird.example.Memcache.client")
  )
}

def module(name: String) = {
  val id = "summingbird-%s".format(name)
  Project(id = id, base = file(id), settings = sharedSettings ++ Seq(
    Keys.name := id,
    mimaPreviousArtifacts := youngestForwardCompatible(name).toSet,
    mimaBinaryIssueFilters ++= ignoredABIProblems
  ))
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
  libraryDependencies += "com.twitter" %% "algebird-core" % algebirdVersion
)

lazy val summingbirdOnline = module("online").settings(
  libraryDependencies ++= Seq(
    "com.twitter" %% "algebird-core" % algebirdVersion,
    "com.twitter" %% "algebird-util" % algebirdVersion,
    "com.twitter" %% "bijection-core" % bijectionVersion,
    "com.twitter" %% "bijection-util" % bijectionVersion,
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
    "com.twitter" %% "storehaus-memcache" % storehausVersion,
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
