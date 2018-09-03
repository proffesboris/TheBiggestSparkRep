name := "VDOKROut"

version := "1.0"

assemblyJarName in assembly := "VDOKROut.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)