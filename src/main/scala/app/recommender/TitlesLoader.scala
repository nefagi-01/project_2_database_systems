package app.recommender

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Helper class for loading the input
 *
 * @param sc The Spark context for the given application
 * @param path The path for the input file
 */
class TitlesLoader(sc : SparkContext, path : String) extends Serializable {

  /**
   * Read the title file in the given path and convert it into an RDD
   *
   * @return The RDD for the given titles
   */
  def load(): RDD[(Int, String, List[String])] = {
    val fileRdd = sc.textFile("src/main/resources/" + path)
    val data = fileRdd.map{row => {
      val values : List[String] = row.split('|').toList
      (values.head.toInt, values(1), values.drop(2))
    }}
    data.persist()
    data

  }
}
