name := "testkafka"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.kafka" %% "kafka" % "2.4.1"
libraryDependencies += "org.apache.commons" % "commons-collections4" % "4.4"
libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.30"
libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.30"

val logback = "1.2.3"
libraryDependencies += "ch.qos.logback" % "logback-core" % logback
libraryDependencies += "ch.qos.logback" % "logback-classic" % logback
