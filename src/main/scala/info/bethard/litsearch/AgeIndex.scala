package info.bethard.litsearch

import org.apache.lucene.index.IndexReader
import org.apache.lucene.search.Query
import org.apache.lucene.store.Directory
import org.apache.lucene.queries.function.FunctionQuery
import org.apache.lucene.queries.function.valuesource.IntFieldSource
import org.apache.lucene.queries.function.valuesource.LinearFloatFunction
import org.apache.lucene.queries.function.ValueSource

class AgeIndex(currentYear: Int, scoreAdjuster: (ValueSource => ValueSource)) extends Index {

  override def createQuery(queryText: String): Query = {
    val yearSource = new IntFieldSource(IndexConfig.FieldNames.year)
    val ageSource = new LinearFloatFunction(yearSource, -1f, currentYear.toFloat)
    new FunctionQuery(scoreAdjuster(ageSource))
  }
}