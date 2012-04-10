package info.bethard.litsearch

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.store.FSDirectory

@RunWith(classOf[JUnitRunner])
class CitationCountIndexSuite extends IndexSuiteBase {

  test("index is created with correct citation counts") {
    import IndexConfig.FieldNames.{ articleID, citedArticleIDs }
    for {
      tempReader <- this.temporaryIndexReader(
        Seq(articleID -> "0", citedArticleIDs -> ""),
        Seq(articleID -> "1", citedArticleIDs -> "0"),
        Seq(articleID -> "2", citedArticleIDs -> "1"),
        Seq(articleID -> "3", citedArticleIDs -> "0 1 2"))
      tempDir <- this.temporaryDirectory
    } {

      // construct the index of citation counts
      val index = new CitationCountIndex(FSDirectory.open(tempDir))
      index.buildFrom(tempReader)

      // check the values of the citation counts
      val reader = index.reader
      val searcher = new IndexSearcher(reader)
      assert(searcher.maxDoc() === 4)
      import IndexConfig.FieldNames.citationCount
      assert(searcher.doc(0).get(citationCount) === "2")
      assert(searcher.doc(1).get(citationCount) === "2")
      assert(searcher.doc(2).get(citationCount) === "1")
      assert(searcher.doc(3).get(citationCount) === "0")
      searcher.close
      reader.close
    }
  }

  test("query returns correct citation counts") {
    import IndexConfig.FieldNames.{ articleID, citedArticleIDs }
    for {
      tempReader <- this.temporaryIndexReader(
        Seq(articleID -> "0", citedArticleIDs -> ""),
        Seq(articleID -> "1", citedArticleIDs -> "0"),
        Seq(articleID -> "2", citedArticleIDs -> "1"),
        Seq(articleID -> "3", citedArticleIDs -> "1 2"))
      tempDir <- this.temporaryDirectory
    } {
      // construct the index of citation counts
      val index = new CitationCountIndex(FSDirectory.open(tempDir))
      index.buildFrom(tempReader)

      // check the values of the citation counts
      val reader = index.reader
      val searcher = new IndexSearcher(reader)
      val query = index.query
      val topDocs = searcher.search(query, reader.maxDoc)
      assert(topDocs.totalHits === 4)
      val expectedScores = Array(0 -> 1.0, 1 -> 2.0, 2 -> 1.0, 3 -> 0.0)
      val actualScores = topDocs.scoreDocs.map(d => d.doc -> d.score)
      assert(expectedScores.toMap === actualScores.toMap)
      searcher.close
      reader.close
    }
  }
}
