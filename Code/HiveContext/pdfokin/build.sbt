name := "pdfokin"

version := "0.1"

assemblyJarName in assembly := "pdfokin.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)