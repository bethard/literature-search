package info.bethard.litsearch

import org.apache.lucene.index.IndexReader
import org.apache.lucene.search.Query
import org.apache.lucene.store.Directory

class TextIndex(defaultField: String, directory: Directory) extends Index {

  private val queryParser = IndexConfig.newQueryParser(defaultField)

  def openReader(): IndexReader = IndexReader.open(this.directory)

  def createQuery(queryText: String): Query = queryParser.parse(queryText)
}

class AbstractTextIndex(directory: Directory)
  extends TextIndex(IndexConfig.FieldNames.abstractText, directory)

class TitleTextIndex(directory: Directory)
  extends TextIndex(IndexConfig.FieldNames.titleText, directory)