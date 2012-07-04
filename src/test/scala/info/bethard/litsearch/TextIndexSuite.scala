package info.bethard.litsearch

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.store.FSDirectory
import java.util.Calendar

@RunWith(classOf[JUnitRunner])
class TextIndexSuite extends IndexSuiteBase {

  test("AbstractTextIndex query returns correct articles") {
    import IndexConfig.FieldNames.{ abstractText => text }
    for (tempDir <- this.temporaryFSDirectory) {
      // create the index of abstract texts
      val index = new AbstractTextIndex(tempDir)
      this.writeDocuments(tempDir,
        Seq(text -> "aaa bbb ccc"),
        Seq(text -> "ccc;ddd;eee"),
        Seq(text -> "eee-fff-ggg"),
        Seq(text -> "ggghhhiii"))

      // check some queries
      val reader = index.openReader
      val searcher = new IndexSearcher(reader)
      val expected = Seq("aaa" -> Array(0), "ccc" -> Array(0, 1), "ggg" -> Array(2))
      for ((query, expectedDocs) <- expected) {
        val topDocs = searcher.search(index.createQuery(query), reader.maxDoc)
        assert(topDocs.scoreDocs.map(_.doc) === expectedDocs)
      }
      searcher.close
      reader.close
    }
  }

  test("TitleTextIndex query returns correct articles") {
    import IndexConfig.FieldNames.{ titleText => text }
    for (tempDir <- this.temporaryFSDirectory) {
      // create the index of abstract texts
      val index = new TitleTextIndex(tempDir)
      this.writeDocuments(tempDir,
        Seq(text -> "aaa bbb ccc"),
        Seq(text -> "ccc;ddd;eee"),
        Seq(text -> "eee-fff-ggg"),
        Seq(text -> "ggghhhiii"))

      // check some queries
      val reader = index.openReader
      val searcher = new IndexSearcher(reader)
      val expected = Seq("aaa" -> Array(0), "ccc" -> Array(0, 1), "ggg" -> Array(2))
      for ((query, expectedDocs) <- expected) {
        val topDocs = searcher.search(index.createQuery(query), reader.maxDoc)
        assert(topDocs.scoreDocs.map(_.doc) === expectedDocs)
      }
      searcher.close
      reader.close
    }
  }
}