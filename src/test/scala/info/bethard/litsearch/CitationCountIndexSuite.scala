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
        Seq(articleID -> "3", citedArticleIDs -> "0 2"))
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
      assert(topDocs.scoreDocs(0).score === 2)
      assert(topDocs.scoreDocs(1).score === 1)
      assert(topDocs.scoreDocs(2).score === 1)
      assert(topDocs.scoreDocs(3).score === 0)
      searcher.close
      reader.close
    }
  }
}
