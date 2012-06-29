package info.bethard.litsearch

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.DefaultSimilarity

@RunWith(classOf[JUnitRunner])
class CombinedIndexSuite extends IndexSuiteBase {

  test("index is created with correct citation counts") {
    import IndexConfig.FieldNames.{ articleIDWhenCited, citedArticleIDs, year }
    for {
      tempReader <- this.temporaryIndexReader(
        Seq(articleIDWhenCited -> "0", year -> "1999", citedArticleIDs -> ""),
        Seq(articleIDWhenCited -> "1", year -> "2000", citedArticleIDs -> "0"),
        Seq(articleIDWhenCited -> "2", year -> "2005", citedArticleIDs -> "1"),
        Seq( /* test missing ID */ year -> "2002", citedArticleIDs -> "1 2"))
      citationCountIndexDirectory <- this.temporaryFSDirectory
      ageIndexDirectory <- this.temporaryFSDirectory
    } {

      // construct the index of citation counts
      val citationCountIndex = new CitationCountIndex(citationCountIndexDirectory)
      citationCountIndex.buildFrom(tempReader)

      // construct the index of age counts
      val ageIndex = new AgeIndex(ageIndexDirectory)
      ageIndex.buildFrom(tempReader, 2012)

      // construct the combined index
      val index = new CombinedIndex(citationCountIndex -> 10f, ageIndex -> -0.5f)

      // construct the reader and searcher
      val reader = index.openReader
      val searcher = new IndexSearcher(reader)

      // get rid of the query norm so that scores are directly interpretable
      searcher.setSimilarity(new DefaultSimilarity {
        override def queryNorm(sumOfSquaredWeights: Float) = 1f
      })

      // check that scores are calculated as a weighted sum of the combined queries
      val topDocs = searcher.search(index.createQuery(null), reader.maxDoc)
      assert(topDocs.totalHits === 4)
      val expectedScores = Array(
        0 -> (10f * 1 - 0.5 * 13),
        1 -> (10f * 2 - 0.5 * 12),
        2 -> (10f * 1 - 0.5 * 7),
        3 -> (10f * 0 - 0.5 * 10))
      val actualScores = topDocs.scoreDocs.map(d => d.doc -> d.score)
      assert(expectedScores.toMap === actualScores.toMap)

      // close things we've opened
      searcher.close
      reader.close
    }
  }
}
