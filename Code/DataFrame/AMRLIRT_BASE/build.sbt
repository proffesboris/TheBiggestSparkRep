name := "AMRLIRT_BASE"

version := "0.1"

assemblyJarName in assembly := "amrlirt_base.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)