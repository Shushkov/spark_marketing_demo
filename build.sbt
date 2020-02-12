
name := "spark_marketing_demo"

version := "0.1"

scalaVersion := "2.11.12"

scalacOptions += "-target:jvm-1.8"
javacOptions ++= Seq("-source", "1.8")

scalacOptions ++= Seq("-language:implicitConversions", "-deprecation")
libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-core" % "2.4.4"),
  ("org.apache.spark" %% "spark-sql" % "2.4.4"),
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)
dependencyOverrides ++= Seq(
  ("com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7")
)

assemblyJarName in assembly := s"${name.value.replace(' ','-')}-${version.value}.jar"

assemblyOption in assembly := (assemblyOption in assembly).
  value.copy(includeScala = false)

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}

fork in run := true