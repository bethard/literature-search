package info.bethard.litsearch
import org.apache.lucene.index.IndexReader
import org.apache.lucene.index.ParallelReader
import org.apache.lucene.search.Query
import org.apache.lucene.search.BooleanQuery
import org.apache.lucene.search.BooleanClause

class CombinedIndex(indexWeights: (Index, Float)*) extends Index {

  def reader: IndexReader = {
    val reader = new ParallelReader
    for ((index, weight) <- indexWeights) {
      reader.add(index.reader)
    }
    reader
  }

  def query: Query = {
    val query = new BooleanQuery
    for ((index, weight) <- indexWeights) {
      val subQuery = index.query
      subQuery.setBoost(weight)
      query.add(subQuery, BooleanClause.Occur.MUST)
    }
    query
  }
}
