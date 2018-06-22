package philgbr.exploration.spark.tasks

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.Encoders

import philgbr.exploration.spark.db.MovieLensTables.{RATINGS, getQualifiedName}
import philgbr.exploration.spark.beans._
import scala.beans.BeanProperty
import scala.reflect.ClassTag



object GroupMoviesByRatings {
  
  private val MethodsUnderTest = Array( "hiveImpl1"
                                         , "dfopImpl1", "dfopImpl2"
                                         , "dsImpl1", "dsImpl2"
                                         , "rddImpl1"  // <-- Beware that this implementation does NOT scale  
                                         , "rddImpl2")
                                                                      
  private val QryCountByRatings  = """select rating, count(*) as nb_ratings
                                     | from %s 
                                     | group by rating 
                                     | order by rating""".stripMargin.replaceAll("\n", " ")
}

class GroupMoviesByRatings {
  
  import GroupMoviesByRatings.QryCountByRatings
  
  def hiveImpl1(spark: SparkSession, dbSchemaName: String): Unit = {
    spark.sql(String.format(QryCountByRatings, getQualifiedName(RATINGS, dbSchemaName))).show()
  }
  
  def dfopImpl1(spark: SparkSession, dbSchemaName: String): Unit = {
    spark.table(getQualifiedName(RATINGS, dbSchemaName))
        .select(col("rating"))
        .groupBy(col("rating"))
        .count()
        .orderBy(col("rating"))
        .show();
  }
  
  /** Same as [[dfopImpl1]] except we removed the projection so we can measure the impact of such omission */
  def dfopImpl2(spark: SparkSession, dbSchemaName: String): Unit = {
    spark.table(getQualifiedName(RATINGS, dbSchemaName))
        //.select(col("rating"))  // oups !
        .groupBy(col("rating"))
        .count()
        .orderBy(col("rating"))
        .show();
  }
  
  /** Implementation based on the strongly typed Dataset API */

  def dsImpl1(spark: SparkSession,  dbSchemaName: String ): Unit = {

       val ds = spark.table(getQualifiedName(RATINGS, dbSchemaName))
                     .select("rating")
                     .as(Encoders.FLOAT);

       val groupDs= ds.groupBy(col("rating"));
       val countDf= groupDs.count();
       val sortedDf= countDf.sort(col("rating"));
       
       sortedDf.show();
   }
  
  /** Implementation based on the strongly typed Dataset API, with user defined types encoding */
  def dsImpl2(spark: SparkSession,  dbSchemaName: String ): Unit = {

      val ratingEncoder = Encoders.bean(Rating.getClass)

      // val ds: Dataset[Rating]
      val ds = spark.table(getQualifiedName(RATINGS, dbSchemaName))
                      .selectExpr("movie_id as movieId", "user_id as userId", "rating", "time as timestamp")
                      .as(ratingEncoder);

      // val fds: Dataset[Float]
      val fds=ds.select(col("rating")).as(Encoders.FLOAT);

      // val groupDs: RelationalGroupedDataset 
      val groupDs = fds.groupBy(col("rating"));
      
      // val countDf: Dataset<Row>
      val  countDf= groupDs.count();
      
      // val sortedDf: Dataset<Row>
      val sortedDf= countDf.sort(col("rating"));
     
      sortedDf.show(true);
  }

  /** RDD implementation that <b>DOES NOT scale</b>  because of the use of [[org.apache.spark.rdd.RDD.groupBy]] that MUST NOT be applied to a large dataset.
   *
   * 	Instead, use a method relying on the {@link RDD#reduceByKey(...)} method's family (or any equivalent that performs 
   * 	a first grouping on workers, before shuffling.
   * 
   *  @see #rddImpl2(SparkSession, String)
   */

  def rddImpl1(spark: SparkSession , dbSchemaName: String) : Unit = {
    
    import spark.implicits._      // This import provides our local scope with Encoder for common types ...
  //val ratingEncoder = Encoders.bean(Rating.getClass)  //  ... and hence makes unnecessary defining such an ad-hoc Encoder 


    
    // val typedRdd : RDD[Rating] 
    val typedRdd = spark.table(getQualifiedName(RATINGS, dbSchemaName))
                          .selectExpr("movie_id as movieId", "user_id as userId", "rating", "time as timestamp")
                          .as[Rating]
                        //.as(ratingEncoder)   // alternative using an ad-hoc Encoder
                          .rdd
                                
    //val typedPairRdd: RDD[(Float, Iterable[Rating])]                            
    val typedPairRdd = typedRdd.groupBy[Float]((r: Rating) => (r.rating))

    // val sortedPairRdd: RDD[(Float, Iterable[Rating])]
    val sortedPairRdd = typedPairRdd.sortByKey()
    
    sortedPairRdd.foreach(
        (tuple: (Float, Iterable[Rating])) => (println(s"Rating : ${tuple._1}  Count : ${tuple._2.size}")) )
                          
  }

  /** RDD implementation that scales properly */
  def rddImpl2(spark: SparkSession, dbSchemaName: String ) : Unit = {

    // val typedRdd: RDD[Float]
    val typedRdd = spark.table(getQualifiedName(RATINGS, dbSchemaName))
                          .selectExpr("rating")
                          .as(Encoders.scalaFloat)
                          .rdd
    
    // mapping function (for clarity's sake)
    def mapF = (f: Float) => ((f, 1))
   
    // val typedPairRdd: RDD[(Float, Int)]
    val typedPairRdd = typedRdd.map[(Float, Int)](mapF)
  
    // val countByRating: RDD[(Float, Int)]
    
          // Implicit conversion of 
          //  typedPairRdd 
          //	  ⇒ rddToPairRDDFunctions(typedPairRdd): (
          //	  						  implicit kt: scala.reflect.ClassTag[Float]
          //	  						, implicit vt: scala.reflect.ClassTag[Int]
          //	  						, implicit ord: Ordering[Float]
          //	  						) org.apache.spark.rdd.PairRDDFunctions[Float,Int]
    val countByRating = typedPairRdd.reduceByKey((i1: Int, i2: Int) => i1 + i2);
    
    
    // val countByRating: RDD[(Float, Int)] 
    
          // Implicit conversion of 
          // countByRating 
          //	⇒ rddToOrderedRDDFunctions(countByRating): (
          //							  implicit evidence$35: Ordering[Float]
          //							, implicit evidence$36: scala.reflect.ClassTag[Float]
          //							, implicit evidence$37: scala.reflect.ClassTag[Int]
          //							) org.apache.spark.rdd.OrderedRDDFunctions[Float,Int,(Float, Int)]    
    val sortedCount = countByRating.sortByKey();
    
    //val results: Array[(Float, Int)]
    val results = sortedCount.collect()
    
    results.foreach( (tuple: (Float, Int)) => (println(s"Rating : ${tuple._1}  Count : ${tuple._2}")) )
  }
}
