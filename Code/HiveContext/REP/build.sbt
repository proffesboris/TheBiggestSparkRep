name := "rep"

version := "0.1"

assemblyJarName in assembly := "rep.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)