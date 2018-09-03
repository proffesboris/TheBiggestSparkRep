name := "ProxyLGDIn"

version := "1.0"

assemblyJarName in assembly := "ProxyLGDIn.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)