package info.bethard.litsearch

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.store.FSDirectory

@RunWith(classOf[JUnitRunner])
class CitationCountIndexSuite extends IndexSuiteBase {

  test("index is created with correct citation counts") {
    import IndexConfig.FieldNames.{ articleIDWhenCited, citedArticleIDs }
    for {
      tempReader <- this.temporaryDirectoryReader(
        Seq(articleIDWhenCited --> "0", citedArticleIDs --> ""),
        Seq(articleIDWhenCited --> "1", citedArticleIDs --> "0"),
        Seq(articleIDWhenCited --> "2", citedArticleIDs --> "1"),
        Seq(articleIDWhenCited --> "3", citedArticleIDs --> "0 1 2"))
      tempDir <- this.temporaryFSDirectory
    } {

      // construct the index of citation counts
      CitationCountIndex.buildFrom(tempReader, tempDir)

      // check the values of the citation counts
      val reader = DirectoryReader.open(tempDir)
      val searcher = new IndexSearcher(reader)
      assert(reader.maxDoc() === 4)
      import IndexConfig.FieldNames.citationCount
      assert(searcher.doc(0).get(citationCount) === "2")
      assert(searcher.doc(1).get(citationCount) === "2")
      assert(searcher.doc(2).get(citationCount) === "1")
      assert(searcher.doc(3).get(citationCount) === "0")
      reader.close
    }
  }

  test("query returns correct citation counts") {
    import IndexConfig.FieldNames.{ articleIDWhenCited, citedArticleIDs }
    for {
      tempReader <- this.temporaryDirectoryReader(
        Seq(articleIDWhenCited --> "0", citedArticleIDs --> ""),
        Seq(articleIDWhenCited --> "1", citedArticleIDs --> "0"),
        Seq(articleIDWhenCited --> "2", citedArticleIDs --> "1"),
        Seq(articleIDWhenCited --> "3", citedArticleIDs --> "1 2"))
      tempDir <- this.temporaryFSDirectory
    } {
      // construct the index of citation counts
      CitationCountIndex.buildFrom(tempReader, tempDir)

      // check the values of the citation counts
      val index = new CitationCountIndex(identity)
      val reader = DirectoryReader.open(tempDir)
      val searcher = new IndexSearcher(reader)
      val query = index.createQuery(null) // shouldn't use query text
      val topDocs = searcher.search(query, reader.maxDoc)
      assert(topDocs.totalHits === 4)
      val expectedScores = Array(0 -> 1.0, 1 -> 2.0, 2 -> 1.0, 3 -> 0.0)
      val actualScores = topDocs.scoreDocs.map(d => d.doc -> d.score)
      assert(expectedScores.toMap === actualScores.toMap)
      reader.close
    }
  }
}
