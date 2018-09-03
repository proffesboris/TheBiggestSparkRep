name := "Score"

version := "0.1"

assemblyJarName in assembly := "Score.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)