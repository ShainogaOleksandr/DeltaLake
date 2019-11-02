package tutorial.quickstart
import org.apache.spark.sql._
import utils.Utils

object TestPartitioning {
  def main(args: Array[String]): Unit = {
    val spark = Utils.getSparkSession
    val outputPath = Utils.getOutputFolder + "/TestPartitioning"
    import spark.implicits._
    val df = Utils.getMoviesDF().withColumn("partitionColun", $"movieId" % 10)
//      .withColumn("partitionColun_2", $"movieId" % 3)
//    df.write.format("delta").partitionBy("partitionColun").save(outputPath)


//    df.write.format("delta").mode(SaveMode.Overwrite).partitionBy("partitionColun_2").save(outputPath)


    df.where("partitionColun == 2.0").write
      .format("delta")
      .mode("overwrite")
      .option("replaceWhere", "partitionColun == 2.0")
      .save(outputPath)
    //    df.write.format("delta").mode(SaveMode.Overwrite)
//      .partitionBy("partitionColun","partitionColun_2")
//      .save(outputPath)

  }
}
