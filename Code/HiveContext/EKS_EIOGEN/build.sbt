name := "Eks_Eiogen"

version := "0.1"

assemblyJarName in assembly := "EksEiogen.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
        