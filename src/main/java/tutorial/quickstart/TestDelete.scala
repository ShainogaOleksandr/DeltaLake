package tutorial.quickstart

import io.delta.tables._
import org.apache.spark.sql.functions
import utils.Utils

object TestDelete {
  def main(args: Array[String]): Unit = {

    val spark = Utils.getSparkSession
    val outputPath = Utils.getOutputFolder + "/testDelete"
    import spark.implicits._
    val df = Utils.getMoviesDF()

    df.write.format("delta").save(outputPath)

    val deltaTable = DeltaTable.forPath(outputPath)


    deltaTable.delete(condition = functions.expr("movieId  == 7"))

    spark.read.parquet(outputPath+"/part-00000-d128a2cb-d352-42be-814f-3f4571e4f7a9-c000.snappy.parquet").show()

    spark.read.parquet(outputPath+"/part-00000-a07f5181-4ee3-4bde-a85d-d30ee3c1c4ab-c000.snappy.parquet").show()
  }


}
