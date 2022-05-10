package app.aggregator

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Class for computing the aggregates
 *
 * @param sc The Spark context for the given application
 */
class Aggregator(sc : SparkContext) extends Serializable {

  var state: RDD[(String, Double, List[String])] = null

  /**
   * Use the initial ratings and titles to compute the average rating for each title.
   * The average rating for unrated titles is 0.0
   *
   * @param ratings The RDD of ratings in the file
   * @param title   The RDD of titles in the file
   */
  def init(
            ratings : RDD[(Int, Int, Option[Double], Double, Int)],
            title : RDD[(Int, String, List[String])]
          ) : Unit = {
   /* ratings.foreach(println(_))
    val tmp1 = ratings.groupBy(tuple => (tuple._1, tuple._2))
    println("tmp1:")
    tmp1.foreach(println(_))
    val tmp2 = tmp1.mapValues(values => values.toSeq.sortBy(tupleValues => tupleValues._4))
    println("tmp2:")
    tmp2.foreach(println(_))
    val tmp3 = tmp2.flatMap(pair => (pair._2.head._2, pair._2.head._4).asInstanceOf[TraversableOnce[(Int,Double)]])
    println("tmp3:")
    tmp3.foreach(println(_))
    val tmp4 = tmp3.groupBy(tuple => tuple._1).mapValues(ratings => ratings.aggregate((0.asInstanceOf[Double], 0))(
      (x,y) => (x._1+(y._2), x._2 + 1),
      (x,y) => (x._1+y._1, x._2+y._2)
    ))
    println("tmp4:")
    tmp4.foreach(println(_))
    val tmp5 = tmp4.flatMap(pair => (pair._1, pair._2._1/pair._2._2).asInstanceOf[TraversableOnce[(Int, Double)]])
    println("tmp5:")
    tmp5.foreach(println(_))
    val tmp6 = tmp5.map(pair => (pair._1, pair))
    println("tmp6:")
    tmp6.foreach(println(_))
    println("--------------------------------------------")*/
    val averageRating = ratings.groupBy(tuple => (tuple._1, tuple._2)).mapValues(values => values.toSeq.sortBy(tupleValues => tupleValues._4)).flatMap(pair => (pair._2.head._2, pair._2.head._4).asInstanceOf[TraversableOnce[(Int,Double)]]).groupBy(tuple => tuple._1).mapValues(ratings => ratings.aggregate((0.asInstanceOf[Double], 0))(
      (x,y) => (x._1+y._2, x._2 + 1),
      (x,y) => (x._1+y._1, x._2+y._2)
    )).flatMap(pair => (pair._1, pair._2._1/pair._2._2).asInstanceOf[TraversableOnce[(Int, Double)]]).map(pair => (pair._1, pair))
    val title2 = title.map(tuple => (tuple._1, tuple))
    val join = title2.leftOuterJoin(averageRating).map(result => (result._2._1._2, result._2._2 match {
      case Some(x) => x._2
      case None => 0
    }, result._2._1._3))

    state = join

    state.persist()
  }

  /**
   * Return pre-computed title-rating pairs.
   *
   * @return The pairs of titles and ratings
   */
  def getResult() : RDD[(String, Double)] = {
    state.map(tuple => (tuple._1, tuple._2))
  }

  /**
   * Compute the average rating across all (rated titles) that contain the
   * given keywords.
   *
   * @param keywords A list of keywords. The aggregate is computed across
   *                 titles that contain all the given keywords
   * @return The average rating for the given keywords. Return 0.0 if no
   *         such titles are rated and -1.0 if no such titles exist.
   */
  def getKeywordQueryResult(keywords : List[String]) : Double = ???

  /**
   * Use the "delta"-ratings to incrementally maintain the aggregate ratings
   *
   * @param delta Delta ratings that haven't been included previously in aggregates
   */
  def updateResult(delta_ : Array[(Int, Int, Option[Double], Double, Int)]) : Unit = ???
}
