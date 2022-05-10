package app.recommender

import org.apache.spark.rdd.RDD

/**
 * Class for performing LSH lookups
 *
 * @param lshIndex A constructed LSH index
 */
class NNLookup(lshIndex: LSHIndex) extends Serializable {

  /**
   * Lookup operation for queries
   *
   * @param input The RDD of keyword lists
   * @return The RDD of (keyword list, resut) pairs
   */
  def lookup(queries: RDD[List[String]])
  : RDD[(List[String], List[(Int, String, List[String])])] = {
    println("queries:")
    queries.foreach(println(_))
    val hashed = lshIndex.hash(queries)
    println("hashed: ")
    hashed.foreach(println(_))
    val retrieved = lshIndex.lookup(hashed).map(el => (el._2, el._3))
    println("retrieved: ")
    retrieved.foreach(println(_))
    retrieved
  }
}
