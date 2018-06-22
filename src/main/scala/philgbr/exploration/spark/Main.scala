package philgbr.exploration.spark

import org.apache.spark.sql.SparkSession
import philgbr.exploration.spark.tasks.GroupMoviesByRatings

object Main {
  
  
  def main(args: Array[String]): Unit = {
    
    val spark = initSession()
    val task = new GroupMoviesByRatings()
    
//    println("<===================     start exec:  hiveImpl1     ======================> ")
//    task.hiveImpl1(spark, "movielens")
//    println("<===================     end exec:    hiveImpl1     ======================> ")
//
//    
//    println("<===================     start exec:  dfopImpl1     ======================> ")
//    task.dfopImpl1(spark, "movielens")
//    println("<===================     end exec:    dfopImpl1     ======================> ")
//
//    println("<===================     start exec:  dfopImpl12    ======================> ")
//    task.dfopImpl2(spark, "movielens")
//    println("<===================     end exec:    dfopImpl12     ======================> ")
//    
//
//    println("<===================     start exec:  dsImpl1     ======================> ")
//    task.dsImpl1(spark, "movielens")
//    println("<===================     end exec:    dsImpl1     ======================> ")
//
//    println("<===================     start exec:  dsImpl12    ======================> ")
//    task.dsImpl2(spark, "movielens")
//    println("<===================     end exec:    dsImpl12     ======================> ")
    

    println("<===================     start exec:  rddImpl2    ======================> ")
    task.rddImpl2(spark, "movielens")
    println("<===================     end exec:    rddImpl2     ======================> ")
    
  } 
  
  
  private def initSession() : SparkSession = {
    SparkSession.builder().appName(this.getClass.getName).enableHiveSupport().getOrCreate()
  }
}