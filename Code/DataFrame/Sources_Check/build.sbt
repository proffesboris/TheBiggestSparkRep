name := "Sources_Check"

version := "0.1"

assemblyJarName in assembly := "sources_check.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)