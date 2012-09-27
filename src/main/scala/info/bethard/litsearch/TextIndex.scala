package info.bethard.litsearch

import org.apache.lucene.search.BooleanClause
import org.apache.lucene.search.BooleanQuery
import org.apache.lucene.search.Query

class TextIndex(defaultField: String) extends Index {

  def createQuery(queryText: String): Query = IndexConfig.parseTermsQuery(defaultField, queryText)
}

class AbstractTextIndex
  extends TextIndex(IndexConfig.FieldNames.abstractText)

class TitleTextIndex
  extends TextIndex(IndexConfig.FieldNames.titleText)

class TitleAbstractTextIndex extends Index {

  val subIndexes = Seq(new TitleTextIndex, new AbstractTextIndex)

  def createQuery(queryText: String): Query = {
    val query = new BooleanQuery
    for (index <- this.subIndexes) {
      query.add(index.createQuery(queryText), BooleanClause.Occur.SHOULD)
    }
    query
  }
}