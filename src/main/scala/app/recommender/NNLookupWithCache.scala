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
  var histogram: Map[IndexedSeq[Int], Int] = Map.empty[IndexedSeq[Int], Int]

  /**
   * The operation for building the cache
   *
   * @param sc Spark context for current application
   */
  def build(sc : SparkContext) = {
    val n = ((histogram.keys.size * 99) / 100).floor.toInt
    println("histogram size:")
    println(histogram.keys.size)
    println("n: ")
    println(n)
    val signatureCandidates = sc.parallelize(histogram.toSeq.sortWith(_._2 > _._2).take(n))
    val preCache: Map[IndexedSeq[Int], List[(Int, String, List[String])]] = lshIndex.lookup(signatureCandidates).map(el => (el._1, el._3)).collectAsMap().asInstanceOf[Map[IndexedSeq[Int], List[(Int, String, List[String])]]]
    cache = sc.broadcast(preCache)
    histogram = Map.empty[IndexedSeq[Int], Int]

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
    println("alive0")
    val signatures = lshIndex.hash(queries)
    println("alive1")
    //update histogram
    signatures.foreach(signature => {
      if (histogram.contains(signature._1)) {
        histogram += (signature._1 -> (histogram(signature._1)+1))
      } else {
        histogram += (signature._1 -> 1)
      }
    })
    println("alive2")
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
    println("im here")
    println("cache: ")
    cache.value.foreach(println(_))
    val (cacheHit, cacheMiss) = cacheLookup(queries)
    val cacheMissResult = lshIndex.lookup(cacheMiss).map(result => (result._2, result._3))
    println("aa")
    if (cacheHit.isEmpty() || cacheHit == null) {
       cacheMissResult
    }
    else {
       cacheHit ++ cacheMissResult
    }
  }
}
