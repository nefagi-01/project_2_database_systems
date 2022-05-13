package app.aggregator
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Class for computing the aggregates
 *
 * @param sc The Spark context for the given application
 */
class Aggregator(sc : SparkContext) extends Serializable {

  var state: RDD[(Int, String, Double, Int, List[String])] = null

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
    val averageRating = ratings.groupBy(tuple => (tuple._1, tuple._2)).mapValues(values => values.toSeq.sortBy(_._5)(Ordering[Int].reverse)).map(pair => pair._2.head).map(tuple => (tuple._2, tuple._4)).groupBy(tuple => tuple._1).mapValues(ratings => ratings.aggregate((0.asInstanceOf[Double], 0))(
      (x,y) => (x._1+y._2, x._2 + 1),
      (x,y) => (x._1+y._1, x._2+y._2)
    )).map(pair => (pair._1, pair._2._1/pair._2._2, pair._2._2)).map(pair => (pair._1, pair))
    val title2 = title.map(tuple => (tuple._1, tuple))
    val join = title2.leftOuterJoin(averageRating).map(result => (result._1,result._2._1._2, result._2._2 match {
      case Some(x) => x._2
      case None => 0
    }, result._2._2 match {
      case Some(x) => x._3
      case None => 0
    },result._2._1._3))

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

    val filteredByKeywords = state.filter(rating => keywords.forall(rating._5.contains(_)))
    if (filteredByKeywords.count() == 0) -1.0
    else {
      val filteredByRating = filteredByKeywords.filter(rating => rating._3 > 0.0)
      if (filteredByRating.count() == 0) 0.0
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
      getResult().foreach(println(_))
/*
      println("UPDATE:")
*/
      var myDelta = delta_
      state.unpersist()
      /*println("\ndelta_: ")
      delta_.foreach(println(_))
      println("\nstate before:")
      state.foreach(println(_))*/
      state = state.map(rating => {
        val titleUpdate = myDelta.filter(newRating => newRating._2 == rating._1)
        var baseRating = rating
        if (titleUpdate.length > 0) {
          for (newRating <- titleUpdate) {
            if (newRating._3.isEmpty) {
              val newAverage: Double = (baseRating._3 * baseRating._4 + newRating._4) / (baseRating._4 + 1)
              baseRating = (baseRating._1, baseRating._2, newAverage, baseRating._4 + 1, baseRating._5)
            }
            else {
              val newAverage: Double = (baseRating._3 * baseRating._4 + newRating._4 - newRating._3.get) / (baseRating._4)
              baseRating = (baseRating._1, baseRating._2, newAverage, baseRating._4, baseRating._5)
            }
          }
        }
        baseRating
      }
      )
      /*println("\nstate after:")
      state.foreach(println(_))*/
      state.persist()
  }
}


/*myDelta.find(x => x._2 == rating._1) match {
          case Some(newRating) => {
            myDelta = myDelta diff List(newRating)
            if (newRating._3.isEmpty) {
              val newAverage: Double = (rating._3 * rating._4 + newRating._4) / (rating._4 + 1)
              (rating._1, rating._2, newAverage, rating._4 + 1, rating._5)
            }
            else {
              val newAverage: Double = (rating._3 * rating._4 + newRating._4 - newRating._3.get) / (rating._4)
              (rating._1, rating._2, newAverage, rating._4, rating._5)
            }
          }
          case None => rating
        }*/