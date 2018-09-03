name := "Tests"

version := "0.1"

assemblyJarName in assembly := "tests.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
