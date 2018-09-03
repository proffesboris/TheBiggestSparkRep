name := "CRM_Base"

version := "0.1"

assemblyJarName in assembly := "crm_base.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)