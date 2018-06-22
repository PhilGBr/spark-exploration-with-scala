package philgbr.exploration.spark.db

import org.apache.spark.sql.types.{StringType, StructType, StructField, DataTypes, Metadata}
import java.util.HashMap
import org.apache.commons.lang3.StringUtils

object MovieLensTables {

	def  getQualifiedName(table: Table, dbSchemaName : String) : String = {
		if (StringUtils.isNotBlank(dbSchemaName)) {
      String.format("%s.%s", dbSchemaName, table.tableName)
		} else {
		    table.tableName
		}
	}
  
  sealed abstract class Table (val tableName : String, val structure : StructType)  {
      override def toString = tableName

  }
  case object MOVIES  extends Table("movies"
                                    , new StructType(
                                        Array(new StructField("movie_id", DataTypes.IntegerType, false, Metadata.empty),
								                              new StructField("title",    DataTypes.StringType, false,  Metadata.empty),
								                              new StructField("genres",   DataTypes.StringType, false,  Metadata.empty))))
  case object RATINGS  extends Table("ratings"
                                    , new StructType(
                                        Array(new StructField("user_id",  DataTypes.IntegerType, false, Metadata.empty),
                                							new StructField("movie_id", DataTypes.IntegerType, false, Metadata.empty),
                                							new StructField("rating",   DataTypes.FloatType, false,   Metadata.empty),
                                							new StructField("time",     DataTypes.IntegerType, false, Metadata.empty))))
  case object LINKS  extends Table("links"
                                    , new StructType(
                                        Array(new StructField("movie_id", DataTypes.IntegerType, false, Metadata.empty),
                              								new StructField("imdb_id",  DataTypes.IntegerType, false, Metadata.empty),
                              								new StructField("tmdb_id",  DataTypes.IntegerType, false, Metadata.empty))))
  case object TAGS  extends Table("tags"
                                    , new StructType(
                                        Array(new StructField("user_id",  DataTypes.IntegerType, false, Metadata.empty),
                              								new StructField("movie_id", DataTypes.IntegerType, false, Metadata.empty),
                              								new StructField("tag",      DataTypes.StringType, false,  Metadata.empty),
                              								new StructField("time",     DataTypes.IntegerType, false, Metadata.empty))))
  case object GENOME_SCORES  extends Table("genome_scores"
                                    , new StructType(
                                        Array(new StructField("movie_id",  DataTypes.IntegerType, false, Metadata.empty),
                              								new StructField("tag_id",    DataTypes.IntegerType, false, Metadata.empty),
                              								new StructField("relevance", DataTypes.FloatType, false,   Metadata.empty))))
  case object GENOME_TAGS  extends Table("genome_tags"
                                    , new StructType(
                                        Array(new StructField("tag_id", DataTypes.IntegerType, false, Metadata.empty),
								                              new StructField("tag", DataTypes.StringType, false, Metadata.empty))))
		
  case object USERS  extends Table("users"
                                    , new StructType(
                                        Array(new StructField("user_id", DataTypes.IntegerType, false, Metadata.empty),
                              								new StructField("segment_rater", DataTypes.StringType, false, Metadata.empty),
                              								new StructField("segment_tagger", DataTypes.StringType, false, Metadata.empty))))
}
