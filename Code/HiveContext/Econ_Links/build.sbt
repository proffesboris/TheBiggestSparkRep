name := "Econ_Links"

version := "0.1"

assemblyJarName in assembly := "econ_links.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)