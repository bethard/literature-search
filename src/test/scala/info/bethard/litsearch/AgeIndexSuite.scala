package info.bethard.litsearch

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.store.FSDirectory
import java.util.Calendar

@RunWith(classOf[JUnitRunner])
class AgeIndexSuite extends IndexSuiteBase {

  test("query returns correct ages") {
    import IndexConfig.FieldNames.year
    for {
      reader <- this.temporaryDirectoryReader(
        Seq(year -> "2000"),
        Seq(year -> "2009"),
        Seq(year -> "2012"),
        Seq(year -> "1993"))
    } {
      // construct the index of ages
      val index = new AgeIndex(2020)

      // check the values of the ages
      val searcher = new IndexSearcher(reader)
      val query = index.createQuery(null) // shouldn't use query text
      val topDocs = searcher.search(query, reader.maxDoc)
      assert(topDocs.totalHits === 4)
      val expectedScores = Array(0 -> 20.0, 1 -> 11.0, 2 -> 8.0, 3 -> 27.0)
      val actualScores = topDocs.scoreDocs.map(d => d.doc -> d.score)
      assert(expectedScores.toMap === actualScores.toMap)
    }
  }
}
