name := "radioproject"

scalaVersion := "2.12.11"

val sparkVersion = "3.0.0"

libraryDependencies ++= Seq( 
	 "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
 	 "org.apache.spark" %% "spark-core" % sparkVersion,
  	 "org.apache.spark" %% "spark-streaming" % sparkVersion,
  	 "org.apache.spark" %% "spark-sql" % sparkVersion,	 
  	 "org.mongodb.spark" %% "mongo-spark-connector" % "2.4.2",
  	 "org.json4s" %% "json4s-native" % "3.6.9",
  	 "net.manub" %% "scalatest-embedded-kafka" % "0.16.0" % "test",
  	 "org.scalatest" %% "scalatest" % "3.2.0" % "test",
  	 "org.scalactic" %% "scalactic" % "3.2.0",
  	 "org.apache.kafka" %% "kafka" % "2.5.0",
  	 "org.mongodb.scala" %% "mongo-scala-driver" % "2.9.0"
)

parallelExecution in Test := false

assemblyJarName in assembly := "transcriptProcessor.jar"

assemblyMergeStrategy in assembly := {
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
    case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
    case PathList("org", "apache", xs @ _*) => MergeStrategy.last
    case PathList("com", "google", xs @ _*) => MergeStrategy.last
    case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
    case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
    case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
    case PathList("org", "glassfish", xs @ _*) => MergeStrategy.last
    case PathList("com", "fasterxml", xs @ _*) => MergeStrategy.last
    case PathList("org", "aopalliance", xs @ _*) => MergeStrategy.last
    case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
    case PathList("io", "netty", xs @ _*) => MergeStrategy.last
    case "git.properties" => MergeStrategy.rename
    case "module-info.class" => MergeStrategy.rename
    case "about.html" => MergeStrategy.rename
    case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
    case "META-INF/mailcap" => MergeStrategy.last
    case "META-INF/mimetypes.default" => MergeStrategy.last
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case "plugin.properties" => MergeStrategy.last
    case "log4j.properties" => MergeStrategy.last
    case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
}
