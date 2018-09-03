name := "bo_final"

version := "1.0"

assemblyJarName in assembly := "bo_final.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)