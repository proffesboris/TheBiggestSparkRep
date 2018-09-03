name := "CLF"

version := "0.1"

assemblyJarName in assembly := "CLF.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)