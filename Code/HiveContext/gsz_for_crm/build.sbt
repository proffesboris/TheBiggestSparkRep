name := "gsz_for_crm"

version := "0.1"

assemblyJarName in assembly := "gsz_for_crm.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)