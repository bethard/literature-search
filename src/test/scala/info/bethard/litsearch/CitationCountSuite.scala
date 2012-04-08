package info.bethard.litsearch

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.apache.lucene.search.IndexSearcher
import java.io.File
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.document.Document
import org.apache.lucene.document.Field
import org.apache.lucene.index.IndexReader

@RunWith(classOf[JUnitRunner])
class CitationCountSuite extends FunSuite {

  test("index is created with correct citation counts") {
    val origIndexDir = this.tempIndexDirectory
    val citationCountIndexDir = this.tempIndexDirectory

    try {
      // construct a simple index of articles
      import IndexConfig.FieldNames.{ articleID, citedArticleIDs }
      val origReader = this.buildIndex(
        origIndexDir,
        Seq(articleID -> "0", citedArticleIDs -> ""),
        Seq(articleID -> "1", citedArticleIDs -> "0"),
        Seq(articleID -> "2", citedArticleIDs -> "1"),
        Seq(articleID -> "3", citedArticleIDs -> "0 1 2"))

      // construct the index of citation counts
      val index = new CitationCountIndex(FSDirectory.open(citationCountIndexDir))
      index.buildFrom(origReader)
      origReader.close

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

    } finally {
      origIndexDir.delete()
      citationCountIndexDir.delete()
    }
  }

  test("query returns correct citation counts") {
    val origIndexDir = this.tempIndexDirectory
    val citationCountIndexDir = this.tempIndexDirectory

    try {
      // construct a simple index of articles
      import IndexConfig.FieldNames.{ articleID, citedArticleIDs }
      val origReader = this.buildIndex(
        origIndexDir,
        Seq(articleID -> "0", citedArticleIDs -> ""),
        Seq(articleID -> "1", citedArticleIDs -> "0"),
        Seq(articleID -> "2", citedArticleIDs -> "1"),
        Seq(articleID -> "3", citedArticleIDs -> "0 2"))

      // construct the index of citation counts
      val index = new CitationCountIndex(FSDirectory.open(citationCountIndexDir))
      index.buildFrom(origReader)
      origReader.close

      // check the values of the citation counts
      val reader = index.reader
      val searcher = new IndexSearcher(reader)
      val query = index.query
      val topDocs = searcher.search(query, reader.maxDoc)
      assert(topDocs.totalHits === 4)
      assert(topDocs.scoreDocs(0).score == 2)
      assert(topDocs.scoreDocs(1).score == 1)
      assert(topDocs.scoreDocs(2).score == 1)
      assert(topDocs.scoreDocs(3).score == 0)
      searcher.close
      reader.close

    } finally {
      origIndexDir.delete()
      citationCountIndexDir.delete()
    }
  }

  private def tempIndexDirectory: File = {
    val dir = File.createTempFile("lucene", "index")
    dir.delete()
    dir.mkdirs()
    dir
  }

  private def buildIndex(indexDir: File, docs: Seq[(String, String)]*): IndexReader = {
    val fsDir = FSDirectory.open(indexDir)
    val writer = IndexConfig.newIndexWriter(fsDir)
    for (doc <- docs) {
      val document = new Document
      for ((key, value) <- doc) {
        document.add(new Field(key, value, Field.Store.YES, Field.Index.ANALYZED))
      }
      writer.addDocument(document)
    }
    writer.close
    IndexReader.open(fsDir)
  }
}