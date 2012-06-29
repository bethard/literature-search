package info.bethard.litsearch

import org.apache.lucene.index.IndexReader
import org.apache.lucene.search.Query
import org.apache.lucene.store.Directory

class AbstractTextIndex(directory: Directory) extends Index {

  private val queryParser = IndexConfig.newQueryParser(IndexConfig.FieldNames.abstractText)

  def openReader(): IndexReader = IndexReader.open(this.directory)

  def createQuery(queryText: String): Query = queryParser.parse(queryText)

}