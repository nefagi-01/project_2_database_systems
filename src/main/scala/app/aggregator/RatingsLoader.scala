package app.aggregator

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Helper class for loading the input
 *
 * @param sc The Spark context for the given application
 * @param path The path for the input file
 */
class RatingsLoader(sc : SparkContext, path : String) extends Serializable {

  /**
   * Read the rating file in the given path and convert it into an RDD
   *
   * @return The RDD for the given ratings
   */
  def load() : RDD[(Int, Int, Option[Double], Double, Int)] = {
    val fileRdd = sc.textFile("src/main/resources/" + path)

    val data = fileRdd.map{row => {
      val values : List[String] = row.split('|').toList
      (values.head.toInt, values(1).toInt, values(2).toDouble, values(3).toInt)
    }}.groupBy(tuple => (tuple._1, tuple._2)).mapValues(values => {
      val sorted = values.toSeq.sortBy(tupleValues => tupleValues._4)
      var result = Seq.empty[(Int, Int, Option[Double], Double, Int)]
      var previous : Double = Double.NaN
      sorted.foreach(el => {
        if (previous.isNaN) {
          result = result :+ (el._1, el._2, Option.empty[Double], el._3, el._4)
          previous = el._3
        } else{
          val tmp = (el._1, el._2, Option(previous), el._3, el._4)
          previous = el._3
          result = result :+ tmp
        }
      })
      result
    }).flatMap(pair => pair._2)
    data.persist()
    data
  }
}
