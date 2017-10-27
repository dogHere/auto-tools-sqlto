name := "scala-sqlto"

version := "0.1.2"

scalaVersion := "2.12.3"


libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-compiler" % "2.12.3"
)
// https://mvnrepository.com/artifact/org.rosuda.REngine/Rserve
libraryDependencies += "org.rosuda.REngine" % "Rserve" % "1.8.1"

// https://mvnrepository.com/artifact/com.norbitltd/spoiwo_2.12
libraryDependencies += "com.norbitltd" % "spoiwo_2.12" % "1.2.0"

libraryDependencies += "org.apache.commons" % "commons-csv" % "1.4"
// https://mvnrepository.com/artifact/mysql/mysql-connector-java
