name := "sbl"

version := "1.0"

assemblyJarName in assembly := "sbl.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)