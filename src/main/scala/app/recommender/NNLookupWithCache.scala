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
  var book: Map[IndexedSeq[Int], Int] = Map.empty[IndexedSeq[Int], Int]

  /**
   * The operation for building the cache
   *
   * @param sc Spark context for current application
   */
  def build(sc : SparkContext) = ???
  /*{
/*
    ListMap(grades.toSeq.sortBy(_._2):_*)
*/  val n = ((book.keys.size*99)/100).floor.toInt
    val signatureCandidates = book.toSeq.sortWith(_._2 > _._2).take(n)
    signatureCandidates.foreach(signature => {

    })
  }*/

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
    if (cache == null){
      (null, signatures)
    } else {
      val cacheHit = signatures.filter(signature => cache.value.contains(signature._1)).map(signature => (signature._2, cache.value.get(signature._1).get))
      val cacheMiss = signatures.filter(signature => !cache.value.contains(signature._1))
      (cacheHit, cacheMiss)
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
    val (cacheHit, cacheMiss) = cacheLookup(queries)
    val cacheMissResult = lshIndex.lookup(cacheMiss).map(result => (result._2, result._3))
    if (cacheHit.isEmpty() || cacheHit == null) {
       cacheMissResult
    }
    else {
       cacheHit ++ cacheMissResult
    }
    /*val signatures = lshIndex.hash(queries)
    signatures.foreach(signature => {
      if (book.contains(signature._1)) {
        book += (signature._1 -> (book(signature._1)+1))
      } else {
        book += (signature._1 -> 1)
      }
    })*/


  }
}
