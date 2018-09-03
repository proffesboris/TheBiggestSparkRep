name := "bkp"

version := "0.1"

assemblyJarName in assembly := "bkp.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
        