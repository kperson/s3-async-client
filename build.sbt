lazy val s3client = (project in file(".")).
  settings(
    organization := "com.kelt",
    version := "1.0.0",
    scalaVersion := "2.11.12",
    crossScalaVersions := Seq("2.11.12", "2.12.4"),
    publishTo := Some(Resolver.file("file",  new File("releases"))),
    libraryDependencies ++= Seq(
    "org.asynchttpclient"     % "async-http-client" % "2.4.3",
    "org.scala-lang.modules" %% "scala-xml"         % "1.1.0"
))