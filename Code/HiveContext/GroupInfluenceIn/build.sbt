name := "GroupInfluenceIn"

version := "1.0"

assemblyJarName in assembly := "GroupInfluenceIn.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)