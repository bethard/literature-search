package info.bethard.litsearch

import org.apache.lucene.index.IndexReader
import org.apache.lucene.search.Query

trait Index {
  def reader: IndexReader
  def query: Query
}
