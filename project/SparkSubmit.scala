import sbtsparksubmit.SparkSubmitPlugin.autoImport._

object SparkSubmit {
  lazy val settings: Seq[sbt.Def.Setting[_]] =
    SparkSubmitSetting("Main",
        Seq(
          "--class", "philgbr.exploration.spark.Main"
        )
      )
}
