name := "cl_keys"

version := "0.1"

assemblyJarName in assembly := "cl_keys.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
