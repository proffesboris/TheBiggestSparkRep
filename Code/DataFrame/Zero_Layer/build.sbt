name := "Zero_Layer"

version := "0.1"

assemblyJarName in assembly := "zero_layer.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)