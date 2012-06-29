package info.bethard.litsearch

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.store.FSDirectory
import java.util.Calendar

@RunWith(classOf[JUnitRunner])
class AbstractTextIndexSuite extends IndexSuiteBase {

  test("query returns correct articles") {
    import IndexConfig.FieldNames.abstractText
    for (tempDir <- this.temporaryFSDirectory) {
      // create the index of abstract texts
      val index = new AbstractTextIndex(tempDir)
      this.writeDocuments(tempDir,
        Seq(abstractText -> "aaa bbb ccc"),
        Seq(abstractText -> "ccc;ddd;eee"),
        Seq(abstractText -> "eee-fff-ggg"),
        Seq(abstractText -> "ggghhhiii"))

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
