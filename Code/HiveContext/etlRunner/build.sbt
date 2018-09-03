name := "ETL_RUNNER"

version := "0.1"

assemblyJarName in assembly := "etlRunner.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)