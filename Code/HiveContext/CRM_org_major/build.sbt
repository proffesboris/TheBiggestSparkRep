name := "CRM_org_major"

version := "0.1"

assemblyJarName in assembly := "crm_org_major.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)