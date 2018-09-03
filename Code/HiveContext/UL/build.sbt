name := "UL"

version := "0.1"

assemblyJarName in assembly := "UL.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)