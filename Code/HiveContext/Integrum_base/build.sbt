name := "integrum_base"

version := "0.1"

assemblyJarName in assembly := "integrum_base.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
