name := "Basis_Transaction_EKS"

version := "0.1"

assemblyJarName in assembly := "Basis_KSB.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
        