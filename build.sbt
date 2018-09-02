resolvers += "jitpack" at "https://jitpack.io"

name := "movies_rank"
version := "0.0.1"
scalaVersion := "2.11.12"
sparkVersion := "2.3.0"

sparkComponents ++= Seq("sql")
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)