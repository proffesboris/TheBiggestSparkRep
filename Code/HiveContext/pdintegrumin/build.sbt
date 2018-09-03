name := "pdintegrumin"

version := "0.1"

assemblyJarName in assembly := "pdintegrumin.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)