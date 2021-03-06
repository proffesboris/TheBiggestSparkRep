name := "BasisKSB"

version := "0.1"

scalaVersion := "2.11.11"

assemblyJarName in assembly := "Basis_KSB.jar"

val sparkVersion = "2.2.0"

resolvers ++= Seq(
	"apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(

	"org.apache.spark" %% "spark-core" % sparkVersion % Provided,
	"org.apache.spark" %% "spark-sql" % sparkVersion % Provided

)
        
