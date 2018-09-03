name := "rl"

version := "1.0"

assemblyJarName in assembly := "rl.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)