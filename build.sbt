// give the user a nice default project!

lazy val root = (project in file(".")).

  settings(
    inThisBuild(List(
      organization := "philgbr",
      scalaVersion := "2.11.8"
    )),
    name := "spark-exploration-with-scala",
    version := "0.0.1",

    sparkVersion := "2.2.1",
    sparkComponents := Seq(),

    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled"),
    scalacOptions ++= Seq("-deprecation", "-unchecked"),
    parallelExecution in Test := false,
    fork := true,

    coverageHighlighting := true,

    libraryDependencies ++= Seq(
    		
      "org.apache.commons" % "commons-lang3" % "3.7",
      "org.apache.spark" %% "spark-sql" % "2.2.1" % "provided",

      "org.scalatest" %% "scalatest" % "3.0.1" % "test",
      "org.scalacheck" %% "scalacheck" % "1.13.4" % "test",
      "com.holdenkarau" %% "spark-testing-base" % "2.2.1_0.9.0" % "test" 
      


    ),

    // uses compile classpath for the run task, including "provided" jar (cf http://stackoverflow.com/a/21803413/3827)
    run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)).evaluated,

    scalacOptions ++= Seq("-deprecation", "-unchecked"),
    pomIncludeRepository := { x => false },

   resolvers ++= Seq(
      "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
      "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
      "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/",
      Resolver.sonatypeRepo("public")
    ),

    pomIncludeRepository := { x => false }	
  )
  
import sbtassembly.AssemblyPlugin.autoImport._
import sbt.Attributed._
import sbtsparksubmit.SparkSubmitPlugin.autoImport._

//fill in default YARN settings
enablePlugins(SparkSubmitYARN)
//supply default spark configuration
sparkSubmitPropertiesFile := Some(s"${(Keys.resourceDirectory in Compile).value}/spark-defaults.conf")
//submit the assembly jar with all dependencies
sparkSubmitJar := assembly.value.getAbsolutePath
//now blend in the sparkSubmit settings
SparkSubmit.settings
