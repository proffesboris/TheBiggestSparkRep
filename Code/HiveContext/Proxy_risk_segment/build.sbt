name := "proxyRiskSegment"

version := "0.1"

assemblyJarName in assembly := "proxyRiskSegment.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)