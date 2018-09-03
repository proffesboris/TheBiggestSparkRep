name := "skrOff"

version := "0.1"

assemblyJarName in assembly := "skrOff.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
        