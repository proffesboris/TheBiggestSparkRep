name := "bo_prep"

version := "1.0"

assemblyJarName in assembly := "bo_prep.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)