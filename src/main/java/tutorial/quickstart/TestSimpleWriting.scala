package tutorial.quickstart

import utils.Utils

object TestSimpleWriting {
  def main(args: Array[String]): Unit = {
    var spark = Utils.getSparkSession
    val outptuPath = Utils.getOutputFolder+ "/TestSimpleWriting"
    val df = Utils.getMoviesDF()


//        df.write.format("delta").save(outptuPath)
//    df.write.format("delta").mode("overwrite").save(outptuPath)
//
//    df.filter("movieId==2").write.format("delta").mode("overwrite").save(outptuPath)

//    for (_ <- 1 to 11)
//        df.write.format("delta").mode("overwrite").save(outptuPath)

    df.write.format("delta").mode("append").save(outptuPath)




    val df2 = spark.read.format("delta").load(outptuPath)
    df2.show()

  }
}
