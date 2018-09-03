name := "dictLoad"

version := "0.1"

assemblyJarName in assembly := "dictload.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)