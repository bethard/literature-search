package info.bethard.litsearch

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.store.FSDirectory
import java.util.Calendar

@RunWith(classOf[JUnitRunner])
class AgeIndexSuite extends IndexSuiteBase {

  test("index is created with correct ages") {
    import IndexConfig.FieldNames.year
    for {
      tempReader <- this.temporaryIndexReader(
        Seq(year -> "2000"),
        Seq(year -> "2009"),
        Seq(year -> "2012"),
        Seq(year -> "1993"))
      tempDir <- this.temporaryDirectory
    } {

      // construct the index of ages
      val index = new AgeIndex(FSDirectory.open(tempDir))
      index.buildFrom(tempReader, 2012)

      // check the values of the ages
      val reader = index.openReader
      val searcher = new IndexSearcher(reader)
      assert(searcher.maxDoc() === 4)
      import IndexConfig.FieldNames.age
      val currentYear = Calendar.getInstance().get(Calendar.YEAR)
      assert(searcher.doc(0).get(age) === 12.toString)
      assert(searcher.doc(1).get(age) === 3.toString)
      assert(searcher.doc(2).get(age) === 0.toString)
      assert(searcher.doc(3).get(age) === 19.toString)
      searcher.close
      reader.close
    }
  }

  test("query returns correct ages") {
    import IndexConfig.FieldNames.year
    for {
      tempReader <- this.temporaryIndexReader(
        Seq(year -> "2000"),
        Seq(year -> "2009"),
        Seq(year -> "2012"),
        Seq(year -> "1993"))
      tempDir <- this.temporaryDirectory
    } {
      // construct the index of ages
      val index = new AgeIndex(FSDirectory.open(tempDir))
      index.buildFrom(tempReader, 2020)

      // check the values of the ages
      val reader = index.openReader
      val searcher = new IndexSearcher(reader)
      val query = index.createQuery(null) // shouldn't use query text
      val topDocs = searcher.search(query, reader.maxDoc)
      assert(topDocs.totalHits === 4)
      val expectedScores = Array(0 -> 20.0, 1 -> 11.0, 2 -> 8.0, 3 -> 27.0)
      val actualScores = topDocs.scoreDocs.map(d => d.doc -> d.score)
      assert(expectedScores.toMap === actualScores.toMap)
      searcher.close
      reader.close
    }
  }
}