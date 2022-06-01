package app.recommender

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD


/**
 * Class for performing LSH lookups (enhanced with cache)
 *
 * @param lshIndex A constructed LSH index
 */
class NNLookupWithCache(lshIndex : LSHIndex) extends Serializable {
  var cache:  Broadcast[Map[IndexedSeq[Int], List[(Int, String, List[String])]]] = null
  var histogram: RDD[(IndexedSeq[Int], Int)] = null

  /**
   * The operation for building the cache
   *
   * @param sc Spark context for current application
   */
  def build(sc : SparkContext) = {
    val length = histogram.collect().length
    val n = ((length * 99) / 100).floor.toInt
    val preCache= lshIndex.lookup(histogram.zipWithIndex().filter(el => el._2 < (n-1)).map(el => el._1)).map(el => (el._1, el._3)).collectAsMap().toMap
    cache = sc.broadcast(preCache)
  }

  /**
   * Testing operation: force a cache based on the given object
   *
   * @param ext A broadcast map that contains the objects to cache
   */
  def buildExternal(ext : Broadcast[Map[IndexedSeq[Int], List[(Int, String, List[String])]]]) = {
    cache = ext
  }

  /**
   * Lookup operation on cache
   *
   * @param queries The RDD of keyword lists
   * @return The pair of two RDDs
   *         The first RDD corresponds to queries that result in cache hits and
   *         includes the LSH results
   *         The second RDD corresponds to queries that result in cache hits and
   *         need to be directed to LSH
   */
  def cacheLookup(queries: RDD[List[String]])
  : (RDD[(List[String], List[(Int, String, List[String])])], RDD[(IndexedSeq[Int], List[String])]) = {
    val signatures = lshIndex.hash(queries)
    //update histogram
    histogram = signatures.groupBy(el => el._1).mapValues(values => values.size).sortBy(el => el._2, ascending = false).sortBy(el => el._2, ascending = false)

    Option(cache) match {
      case Some(x) =>
        val cacheHit = signatures.filter(signature => x.value.contains(signature._1)).map(signature => (signature._2, x.value.get(signature._1).get))
        val cacheMiss = signatures.filter(signature => !x.value.contains(signature._1))
        (cacheHit, cacheMiss)
      case None =>
        (null, signatures)
    }

  }

  /**
   * Lookup operation for queries
   *
   * @param input The RDD of keyword lists
   * @return The RDD of (keyword list, resut) pairs
   */
  def lookup(queries: RDD[List[String]])
  : RDD[(List[String], List[(Int, String, List[String])])] = {
    val result = cacheLookup(queries)
    val cacheHit = result._1
    val cacheMiss = result._2
    val cacheMissResult = lshIndex.lookup(cacheMiss).map(result => (result._2, result._3))
    if (cacheHit == null || cacheHit.isEmpty()) {
       cacheMissResult
    }
    else {
       cacheHit ++ cacheMissResult
    }
  }
}
