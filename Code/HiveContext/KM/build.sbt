name := "km"

version := "0.1"

assemblyJarName in assembly := "km.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)