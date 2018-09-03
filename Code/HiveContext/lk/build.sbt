name := "LK"

version := "0.1"

assemblyJarName in assembly := "lk.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
