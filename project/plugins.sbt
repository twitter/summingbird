resolvers += Resolver.url("sbt-plugin-releases", new URL("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases/"))(Resolver.ivyStylePatterns)

resolvers ++= Seq(
  "jgit-repo" at "http://download.eclipse.org/jgit/maven"
)

addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.0")
addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.0.0")
addSbtPlugin("com.typesafe" % "sbt-mima-plugin" % "0.1.8")
addSbtPlugin("com.typesafe.sbt" % "sbt-ghpages" % "0.5.3")
addSbtPlugin("com.typesafe.sbt" % "sbt-scalariform" % "1.3.0")
addSbtPlugin("org.scoverage" % "sbt-coveralls" % "1.0.0.BETA1")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.0.4")
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "1.0")
addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.2.2")
addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.6.0")
