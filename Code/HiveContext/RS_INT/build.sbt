name := "rs_int"

version := "1.0"

assemblyJarName in assembly := "rs_int.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)