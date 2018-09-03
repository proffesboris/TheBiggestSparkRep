name := "Basis"

version := "0.1"

assemblyJarName in assembly := "basis.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)