//https://leonard.io/blog/2017/01/an-in-depth-guide-to-deploying-to-maven-central/

lazy val `s3-async-client` = (project in file(".")).
  settings(
    organization := "com.github.kperson",
    version := "1.0.0",
    scalaVersion := "2.11.12",
    crossScalaVersions := Seq("2.11.12", "2.12.4"),
    publishTo := Some(Resolver.file("file",  new File("releases"))),
    libraryDependencies ++= Seq(
        "org.asynchttpclient"     % "async-http-client" % "2.4.3",
        "org.scala-lang.modules" %% "scala-xml"         % "1.1.0"
    ),
    homepage := Some(url("https://github.com/kperson/s3-scala-async")),
    scmInfo := Some(ScmInfo(url("https://github.com/username/projectname"), "git@github.com:kperson/s3-scala-async.git")),
    developers := List (
        Developer(
            "kperson",
            "Kelton Person",
            "kelton.person@gmail.com",
            url("https://github.com/kperson")
        )
    ),
    credentials += Credentials(new File(".credentials")),
    licenses += ("MIT License", url("http://opensource.org/licenses/MIT")),
    publishMavenStyle := true,
    pgpReadOnly := false,
    publishTo := Some (
      if (isSnapshot.value) {
        Opts.resolver.sonatypeSnapshots
      }
      else {
        Opts.resolver.sonatypeStaging
      }
    )
  )