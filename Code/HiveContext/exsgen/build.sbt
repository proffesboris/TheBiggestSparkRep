name := "exsGen"

version := "0.1"

assemblyJarName in assembly := "exsGen.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
        