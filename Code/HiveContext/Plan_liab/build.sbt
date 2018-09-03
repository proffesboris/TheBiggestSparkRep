name := "Plan_liab"

version := "0.1"

assemblyJarName in assembly := "Plan_liab.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)