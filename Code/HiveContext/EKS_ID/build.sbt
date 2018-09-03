name := "EKS_ID"

version := "0.1"

assemblyJarName in assembly := "eks_id.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)