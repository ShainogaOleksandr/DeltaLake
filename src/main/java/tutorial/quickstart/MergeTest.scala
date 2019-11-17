package tutorial.quickstart

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{SaveMode, functions}
import utils.Utils

object MergeTest {
  def main(args: Array[String]): Unit = {
    val spark = Utils.getSparkSession
    val outputPath = Utils.getOutputFolder + "/conditionalUpdate"
    val df = Utils.getMoviesDF().limit(20)
    df.show(false)
    import spark.implicits._
    df.write.mode(SaveMode.Overwrite).format("delta").save(outputPath)

    val dfForUpdate = df.filter("movieId % 2 == 0")
      .withColumn("title",
        functions.concat(functions.lit("new_Prefix "), $"title"))
      .withColumn("genres",
        functions.concat(functions.lit("new_Prefix "), $"genres"))

    val dfForUpdate1 = df.filter("movieId % 2 == 0")
      .withColumn("title",
        functions.concat(functions.lit("new_Prefix "), $"title"))
      .withColumn("genres",
        functions.concat(functions.lit("new_Prefix "), $"genres"))
    //      .withColumn("movieId",
    //        functions.concat(functions.lit("movieId "), $"movieId"))

    val dfForUpdate3 = df.filter("movieId % 2 == 0")
      .withColumn("title1",
        functions.concat(functions.lit("new_Prefix "), $"title"))
      .withColumn("genres2",
        functions.concat(functions.lit("new_Prefix "), $"genres"))
      .withColumn("movieId",
        functions.concat(functions.lit("movieId "), $"movieId")).drop("title")
      .drop("genres")

    val dfForUpdate3_1 = df.filter("movieId >18 == 0")
      .withColumn("title1",
        functions.concat(functions.lit("new_Prefix "), $"title"))
      .withColumn("genres2",
        functions.concat(functions.lit("new_Prefix "), $"genres"))
      .withColumn("movieId",
        functions.concat(functions.lit("movieId "), $"movieId")).drop("title")
      .drop("genres")

    //    DeltaTable.forPath(spark, outputPath).as("movies").merge(
    //      dfForUpdate1.as("updates"), "movies.movieId = updates.movieId"
    //    ).whenMatched().updateExpr(Map("title" -> "updates.genres"))
    //      .whenNotMatched().insertExpr(
    //      Map(
    //        "movieId" -> "updates.genres",
    //        "title" -> "updates.movieId",
    //        "genres" -> "updates.title")).execute()
    dfForUpdate1.show(false)
    df.show(false)

    DeltaTable.forPath(spark, outputPath).as("movies").merge(
      dfForUpdate1.as("updates"), "movies.movieId = updates.movieId"
    )
      .whenMatched("movies.movieId >10")
      .updateAll()
      .whenMatched("movies.movieId >1") // conflict wit previous  when match
      .delete()
      //      .delete()
      .whenNotMatched("movies.movieId =11")
      .insertAll()
      .execute()

    DeltaTable.forPath(spark, outputPath).toDF.show(40, false)
  }
}
