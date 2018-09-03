name := "pim_refresh"

version := "0.1"

assemblyJarName in assembly := "refresh.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)