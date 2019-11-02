package utils

import org.apache.spark.sql.SparkSession

object Utils {
  def getSparkSession:SparkSession={
    val spark=SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
  }

  def getOutputFolder="/home/sawa/Education/delta_lake/src/main/resources/out"

  def getResFolder="/home/sawa/Education/delta_lake/src/main/resources/"

  def getLinksDF()={
    getSparkSession.read.option("header","true").csv(getResFolder+"links.csv")
  }
  def getMoviesDF()={
    getSparkSession.read.option("header","true").csv(getResFolder+"movies.csv")
  }
  def getRatingsDF()={
    getSparkSession.read.option("header","true").csv(getResFolder+"ratings.csv")
  }
  def getTagsDF()={
    getSparkSession.read.option("header","true").csv(getResFolder+"tags.csv")
  }
}
