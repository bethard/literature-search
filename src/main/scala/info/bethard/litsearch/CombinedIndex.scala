package info.bethard.litsearch

import org.apache.lucene.index.CompositeReader
import org.apache.lucene.index.IndexReader
import org.apache.lucene.index.ParallelCompositeReader
import org.apache.lucene.search.Query
import org.apache.lucene.search.BooleanQuery
import org.apache.lucene.search.BooleanClause
import org.apache.lucene.queries.function.BoostedQuery
import org.apache.lucene.queries.function.valuesource.ConstValueSource
import org.apache.lucene.search.Scorer
import scala.collection.JavaConverters._

class CombinedIndex(indexWeights: (Index, Float)*) extends Index {

  def createQuery(queryText: String): Query = {
    val query = new BooleanQuery
    for ((index, weight) <- indexWeights) {
      val subQuery = index.createQuery(queryText)
      val weightedQuery = new BoostedQuery(subQuery, new ConstValueSource(weight))
      query.add(weightedQuery, BooleanClause.Occur.MUST)
    }
    query
  }
}

object CombinedIndex {

  def extractSubScorers(scorer: Scorer): Seq[Scorer] = {
    val childScorers = scorer.getChildren.asScala.toList.map(_.child)
    // hackery to get child scorer not returned by getChildren
    for (childScorer <- childScorers) yield {
      val privateScorerField = childScorer.getClass.getDeclaredField("scorer")
      val isAccessible = privateScorerField.isAccessible
      privateScorerField.setAccessible(true)
      privateScorerField.get(childScorer).asInstanceOf[Scorer]
    }
  }
}
