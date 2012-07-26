package info.bethard.litsearch

import org.apache.lucene.search.Query
import org.apache.lucene.store.Directory

class TextIndex(defaultField: String) extends Index {

  def createQuery(queryText: String): Query = IndexConfig.parseTermsQuery(defaultField, queryText)
}

class AbstractTextIndex
  extends TextIndex(IndexConfig.FieldNames.abstractText)

class TitleTextIndex
  extends TextIndex(IndexConfig.FieldNames.titleText)