package info.bethard.litsearch

import java.io.File
import com.lexicalscope.jewel.cli.{ Option => CliOption }
import org.apache.lucene.store.FSDirectory
import com.lexicalscope.jewel.cli.CliFactory
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.queryParser.QueryParser
import org.apache.lucene.search.Collector
import org.apache.lucene.search.Scorer
import org.apache.lucene.index.IndexReader
import org.apache.lucene.search.BooleanQuery
import scala.collection.JavaConverters._
import org.apache.lucene.search.DefaultSimilarity
import info.bethard.litsearch.IndexConfig.FieldNames
import java.io.PrintWriter

object LearnFeatureWeights {

  trait Options {
    @CliOption(longName = Array("model-dir"), defaultValue = Array("target/model"))
    def getModelDir: File

    @CliOption(longName = Array("title-index"))
    def getTitleIndex: File

    @CliOption(longName = Array("abstract-index"))
    def getAbstractIndex: File

    @CliOption(longName = Array("citation-count-index"))
    def getCitationCountIndex: File

    @CliOption(longName = Array("age-index"))
    def getAgeIndex: File

    @CliOption(longName = Array("n-hits"), defaultValue = Array("100"))
    def getNHits: Int
  }

  def main(args: Array[String]): Unit = {
    val options = CliFactory.parseArguments(classOf[Options], args: _*)
    val nHits = options.getNHits
    val modelDir = options.getModelDir

    // determine names for files in the model directory
    if (!modelDir.exists) {
      modelDir.mkdirs
    }
    val trainingDataFile = new File(options.getModelDir, "training-data.svmmap")

    // create indexes from command line parameters
    implicit def fileToDirectory(file: File) = FSDirectory.open(file)
    val titleIndex = new TitleTextIndex(options.getTitleIndex)
    val abstractIndex = new AbstractTextIndex(options.getAbstractIndex)
    val citationCountIndex = new CitationCountIndex(options.getCitationCountIndex)
    val ageIndex = new AgeIndex(options.getAgeIndex)

    // create main index as weighted sum of other indexes 
    val indexes = Seq(titleIndex, abstractIndex, citationCountIndex, ageIndex)
    val weights = Seq(1f, 1f, 0f, 0f)
    val index = new CombinedIndex(indexes zip weights: _*)

    // open the reader and searcher
    val reader = index.openReader
    val searcher = new IndexSearcher(reader)

    // get rid of the query norm so that scores are directly interpretable
    searcher.setSimilarity(new DefaultSimilarity {
      override def queryNorm(sumOfSquaredWeights: Float) = 1f
    })

    // generate one training example for each document in the index
    val writer = new PrintWriter(trainingDataFile)
    for (doc <- 0 until reader.maxDoc) {
      println("QUERY: " + doc)
      val queryDocument = reader.document(doc)

      // only use articles with an abstract and a bibliography
      val citedIdsOption = Option(queryDocument.get(FieldNames.citedArticleIDs))
      val abstractTextOption = Option(queryDocument.get(FieldNames.abstractText))
      for (citedIdsString <- citedIdsOption; abstractText <- abstractTextOption) {

        // determine what articles were cited
        val citedIds = citedIdsString.split("\\s+").toSet

        // create the query(ies) based on the abstract text
        val queryText = QueryParser.escape(abstractText)
        val query = index.createQuery(queryText)
        val subQueries = indexes.map(_.createQuery(queryText))

        // retrieve other articles based on the main query
        for (scoreDoc <- searcher.search(query, nHits).scoreDocs) {
          val resultDocument = reader.document(scoreDoc.doc)
          val id = resultDocument.get(FieldNames.articleIDWhenCited)

          // determine the sub-scores for each sub-query
          val subScores = subQueries.map { subQuery =>
            val weight = searcher.createNormalizedWeight(subQuery)
            val scorer = weight.scorer(reader, true, true)
            scorer.advance(scoreDoc.doc)
            scorer.score
          }

          // generate SVM-style label and feature string
          val label = if (citedIds.contains(id)) "+1" else "-1"
          val features = subScores.zipWithIndex.map {
            case (score, index) => "%d:%f".format(index, score)
          }
          writer.printf("%s %s\n", label, features.mkString(" "))
        }
      }
    }
    writer.close

    // close the searcher and reader
    searcher.close
    reader.close
  }
}