package app.aggregator
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Class for computing the aggregates
 *
 * @param sc The Spark context for the given application
 */
class Aggregator(sc : SparkContext) extends Serializable {

  var state: RDD[(Int, String, Double, List[String])] = null
  var lastRating: RDD[(Int, Int, Option[Double], Double, Int)] = null

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
    lastRating = ratings.groupBy(tuple => (tuple._1, tuple._2)).mapValues(values => values.toSeq.sortBy(tupleValues => tupleValues._4)).flatMap(pair => (pair._2))
    val averageRating = lastRating.map(tuple => (tuple._2, tuple._4)).groupBy(tuple => tuple._1).mapValues(ratings => ratings.aggregate((0.asInstanceOf[Double], 0))(
      (x,y) => (x._1+y._2, x._2 + 1),
      (x,y) => (x._1+y._1, x._2+y._2)
    )).map(pair => (pair._1, pair._2._1/pair._2._2)).map(pair => (pair._1, pair))
    val title2 = title.map(tuple => (tuple._1, tuple))
    val join = title2.leftOuterJoin(averageRating).map(result => (result._1,result._2._1._2, result._2._2 match {
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
    state.map(tuple => (tuple._2, tuple._3))
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
  def getKeywordQueryResult(keywords : List[String]) : Double = {

    val filteredByKeywords = state.filter(rating => keywords.forall(rating._4.contains(_)))
    if (filteredByKeywords.count() == 0) -1.0
    else {
      val filteredByRating = filteredByKeywords.filter(rating => rating._3 > 0)
      if (filteredByKeywords.count() == 0) 0.0
      else {
        val tmp = filteredByRating.aggregate((0.asInstanceOf[Double], 0))(
          (x,y) => (x._1+y._3, x._2 + 1),
          (x,y) => (x._1+y._1, x._2+y._2)
        )
        val result = tmp._1/tmp._2
        result
      }
    }
  }

  /**
   * Use the "delta"-ratings to incrementally maintain the aggregate ratings
   *
   * @param delta Delta ratings that haven't been included previously in aggregates
   */
  def updateResult(delta_ : Array[(Int, Int, Option[Double], Double, Int)]) : Unit = {
    println("\ndelta list:")
    delta_.foreach(println(_))
    delta_.foreach(newRating => {
      println("\ndelta sample:")
      println(newRating)
      println("\nlastRating before update")
      lastRating.foreach(println(_))
      //Check if exists in lastRating
      if (lastRating.filter(rating => rating._1 == newRating._1 && rating._2 == newRating._2 && rating._5 < newRating._5).count() > 0){
        //User already rated this film

        //Update lastRating
        lastRating = lastRating.map(rating => {
          if (rating._1 == newRating._1 && rating._2 == newRating._2)
            (newRating._1, newRating._2, Option(rating._4), newRating._4, newRating._5)
          else
            rating
        }
        )
      }
      else {
        //User never rated this film

        //Append new rating to lastRating
        lastRating = lastRating.union(sc.parallelize(Seq(newRating)))
      }
      println("\nlastRating after update")
      lastRating.foreach(println(_))
      //Recompute average for the film
      val newAverage = lastRating.filter(rating => rating._2 == newRating._2).map(tuple => (tuple._2, tuple._4)).groupBy(tuple => tuple._1).mapValues(ratings => ratings.aggregate((0.asInstanceOf[Double], 0))(
        (x,y) => (x._1+y._2, x._2 + 1),
        (x,y) => (x._1+y._1, x._2+y._2)
      )).map(pair => (pair._1, pair._2._1/pair._2._2)).map(pair => (pair._1, pair)).collect().head


      println("\nnewAverage: ")
      println(newAverage)

      println("\nstate before update:")
      state.foreach(println(_))
      state = state.map(tuple => {
        if (tuple._1 == newAverage._1) (tuple._1, tuple._2, newAverage._2._2, tuple._4)
        else tuple
      })
      println("\nstate after update:")
      state.foreach(println(_))
      state.persist()
    })
  }
}
