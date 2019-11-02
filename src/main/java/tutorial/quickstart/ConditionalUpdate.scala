package tutorial.quickstart
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions

import utils.Utils

object ConditionalUpdate {
  def main(args: Array[String]): Unit = {
    var spark = Utils.getSparkSession
    val outputPath = Utils.getOutputFolder+ "/conditionalUpdate"
    val df = Utils.getMoviesDF()

//    df.write.format("delta").save(outputPath)

//    val deltaTable = DeltaTable.forPath(outputPath)
//    deltaTable.update(
//      condition = functions.expr("movieId % 2 == 0"),
//      set = Map("movieId" -> functions.expr("movieId + 100")))
//
//    deltaTable.delete(condition = functions.expr("movieId  == 7"))

    spark.read.parquet(outputPath+"/part-00000-bd5ed610-756d-4e89-b861-a6047396d74e-c000.snappy.parquet").show
    spark.read.parquet(outputPath+"/part-00000-6caad560-ba5c-4977-b813-21cc1795be79-c000.snappy.parquet").show
  }
}
