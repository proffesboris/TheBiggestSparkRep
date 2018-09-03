name := "pdpravoin"

version := "0.1"

assemblyJarName in assembly := "pdpravoin.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)