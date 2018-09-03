name := "fok_base"

version := "0.1"

assemblyJarName in assembly := "fok_base.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)