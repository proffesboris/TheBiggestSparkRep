name := "SET"

version := "0.1"

assemblyJarName in assembly := "set.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
