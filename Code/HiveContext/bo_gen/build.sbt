name := "bo_gen"

version := "1.0"

assemblyJarName in assembly := "bo_gen.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)