name := "CLU"

version := "0.1"

assemblyJarName in assembly := "CLU.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)