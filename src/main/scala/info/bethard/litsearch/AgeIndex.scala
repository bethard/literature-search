package info.bethard.litsearch

import org.apache.lucene.index.IndexReader
import org.apache.lucene.search.Query
import org.apache.lucene.store.Directory
import org.apache.lucene.queries.function.FunctionQuery
import org.apache.lucene.queries.function.valuesource.IntFieldSource
import org.apache.lucene.queries.function.valuesource.LinearFloatFunction

class AgeIndex(currentYear: Int) extends Index {

  override def createQuery(queryText: String): Query = {
    val intFieldSource = new IntFieldSource(IndexConfig.FieldNames.year)
    new FunctionQuery(new LinearFloatFunction(intFieldSource, -1f, currentYear.toFloat))
  }
}