name := "acc"

version := "0.1"

assemblyJarName in assembly := "acc.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
        