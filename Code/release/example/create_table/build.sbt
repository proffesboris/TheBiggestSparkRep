name := "create_table"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.0"

artifactName := {(sv: ScalaVersion, module: ModuleID, artifact: Artifact ) => "create_tables.jar"}

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided)
