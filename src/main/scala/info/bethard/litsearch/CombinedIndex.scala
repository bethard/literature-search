package info.bethard.litsearch
import org.apache.lucene.index.IndexReader
import org.apache.lucene.index.ParallelReader
import org.apache.lucene.search.Query
import org.apache.lucene.search.BooleanQuery
import org.apache.lucene.search.BooleanClause

class CombinedIndex(indexWeights: (Index, Float)*) extends Index {

  def openReader(): IndexReader = {
    val reader = new ParallelReader
    for ((index, weight) <- indexWeights) {
      reader.add(index.openReader())
    }
    reader
  }

  def createQuery(queryText: String): Query = {
    val query = new BooleanQuery
    for ((index, weight) <- indexWeights) {
      val subQuery = index.createQuery(queryText)
      subQuery.setBoost(weight)
      query.add(subQuery, BooleanClause.Occur.MUST)
    }
    query
  }
}
