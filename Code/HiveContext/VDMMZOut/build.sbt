name := "VDMMZOut"

version := "1.0"

assemblyJarName in assembly := "VDMMZOut.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)