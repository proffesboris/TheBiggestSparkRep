name := "PDEKSin"

version := "0.1"

assemblyJarName in assembly := "PDEKSin.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)