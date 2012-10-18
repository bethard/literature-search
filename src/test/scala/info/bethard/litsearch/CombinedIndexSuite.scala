package info.bethard.litsearch

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.index.ParallelCompositeReader
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.similarities.DefaultSimilarity

@RunWith(classOf[JUnitRunner])
class CombinedIndexSuite extends IndexSuiteBase {

  test("index is created with correct citation counts") {
    import IndexConfig.FieldNames.{ articleIDWhenCited, citedArticleIDs, year }
    for {
      tempReader <- this.temporaryDirectoryReader(
        Seq(articleIDWhenCited -> "0", year -> "1999", citedArticleIDs -> ""),
        Seq(articleIDWhenCited -> "1", year -> "2000", citedArticleIDs -> "0"),
        Seq(articleIDWhenCited -> "2", year -> "2005", citedArticleIDs -> "1"),
        Seq( /* test missing ID */ year -> "2002", citedArticleIDs -> "1 2"))
      citationCountIndexDirectory <- this.temporaryFSDirectory
    } {
      // construct the index of citation counts
      CitationCountIndex.buildFrom(tempReader, citationCountIndexDirectory)
      val citationCountReader = DirectoryReader.open(citationCountIndexDirectory)
      val citationCountIndex = new CitationCountIndex(identity)

      // construct the index of age counts
      val ageIndex = new AgeIndex(2012, identity)

      // construct the combined index
      val index = new CombinedIndex(citationCountIndex -> 10f, ageIndex -> -0.5f)

      // construct the reader and searcher
      val reader = new ParallelCompositeReader(tempReader, citationCountReader)
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
      reader.close
    }
  }
}
