name := "BBO"

version := "1.0"

assemblyJarName in assembly := "BBO.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)