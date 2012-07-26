package info.bethard.litsearch

import org.apache.lucene.search.Query

trait Index {
  def createQuery(queryText: String): Query
}
