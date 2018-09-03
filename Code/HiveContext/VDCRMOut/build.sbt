name := "VDCRMOut"

version := "1.0"

assemblyJarName in assembly := "VDCRMOut.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)