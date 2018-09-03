name := "Basis_tran"

version := "0.1"

assemblyJarName in assembly := "Basis_tran.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
        