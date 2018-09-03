name := "VDLawRobotOut"

version := "1.0"

assemblyJarName in assembly := "VDLawRobotOut.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)