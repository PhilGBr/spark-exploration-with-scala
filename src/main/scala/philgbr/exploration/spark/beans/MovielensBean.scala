package philgbr.exploration.spark.beans

import scala.beans.BeanProperty

sealed abstract class MovielensBean

case class Link   (
    @BeanProperty movieId: Int, 
    @BeanProperty imdbId: Int, 
    @BeanProperty tmbdId: Int) extends MovielensBean
    
case class Movie  (
    @BeanProperty movieId: Int, 
    @BeanProperty title: String, 
    @BeanProperty genres: String) extends MovielensBean
    
case class Rating (@BeanProperty userId: Int, 
    @BeanProperty movieId: Int, 
    @BeanProperty rating: Float , 
    @BeanProperty timestamp: Int) extends MovielensBean
    
case class Tag    (
    @BeanProperty userId: Int, 
    @BeanProperty movieId: Int,
    @BeanProperty tag: String, 
    @BeanProperty timestamp: Int) extends MovielensBean
    
case class User   (
    @BeanProperty userId: Int, 
    @BeanProperty segmentRater: String, 
    @BeanProperty segmentTagger: String) extends MovielensBean
