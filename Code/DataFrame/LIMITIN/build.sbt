name := "LIMITIN"

version := "0.1"

assemblyJarName in assembly := "limitin.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)