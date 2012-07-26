package info.bethard.litsearch

import com.lexicalscope.jewel.cli.{ Option => CliOption }
import com.lexicalscope.jewel.cli.CliFactory
import java.io.File
import java.io.PrintWriter
import org.apache.lucene.index.AtomicReaderContext
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.index.ParallelCompositeReader
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.Collector
import org.apache.lucene.search.Scorer
import org.apache.lucene.search.BooleanQuery
import org.apache.lucene.search.similarities.DefaultSimilarity
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.util.PriorityQueue
import scala.collection.JavaConverters._
import scala.collection.mutable.Buffer

import info.bethard.litsearch.IndexConfig.FieldNames

object LearnFeatureWeights {

  trait Options {
    @CliOption(longName = Array("model-dir"), defaultValue = Array("target/model"))
    def getModelDir: File

    @CliOption(longName = Array("article-index"))
    def getArticleIndex: File

    @CliOption(longName = Array("citation-count-index"))
    def getCitationCountIndex: File

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
    val titleIndex = new TitleTextIndex
    val abstractIndex = new AbstractTextIndex
    val citationCountIndex = new CitationCountIndex
    val ageIndex = new AgeIndex(2012)

    // create main index as weighted sum of other indexes 
    val indexes = Seq(titleIndex, abstractIndex, citationCountIndex, ageIndex)
    val weights = Seq(1f, 1f, 0.1f, 0.1f)
    val index = new CombinedIndex(indexes zip weights: _*)

    // open the reader and searcher
    val articlesReader = DirectoryReader.open(FSDirectory.open(options.getArticleIndex))
    val citationCountReader = DirectoryReader.open(FSDirectory.open(options.getCitationCountIndex))
    val reader = new ParallelCompositeReader(articlesReader, citationCountReader)
    val searcher = new IndexSearcher(reader)

    // get rid of the query norm so that scores are directly interpretable
    searcher.setSimilarity(new DefaultSimilarity {
      override def queryNorm(sumOfSquaredWeights: Float) = 1f
    })

    // prepare for writing output
    val featureFormat = (1 to weights.size).map(_ + ":%f").mkString(" ")
    val lineFormat = "%s qid:%d %s"

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

        // create the query based on the abstract text
        val query = index.createQuery(abstractText)

        // retrieve other articles based on the main query
        val collector = new DocScoresCollector(nHits)
        searcher.search(query, collector)
        for (docScores <- collector.getDocSubScores) {
          val resultDocument = reader.document(docScores.doc)
          val id = resultDocument.get(FieldNames.articleIDWhenCited)

          // generate SVM-style label and feature string
          val label = if (citedIds.contains(id)) "+1" else "-1"
          val featuresString = featureFormat.format(docScores.subScores: _*)
          writer.println(lineFormat.format(label, doc, featuresString))
        }
      }
    }
    writer.close

    // close the reader
    reader.close
  }
}

case class DocScores(doc: Int, score: Float, subScores: Seq[Float])

class DocScoresPriorityQueue(maxSize: Int) extends PriorityQueue[DocScores](maxSize) {
  def lessThan(a: DocScores, b: DocScores) = a.score < b.score
}

class DocScoresCollector(maxSize: Int) extends Collector {
  var scorer: Scorer = null
  var subScorers: Seq[Scorer] = null
  val priorityQueue = new DocScoresPriorityQueue(maxSize)

  override def setScorer(scorer: Scorer): Unit = {
    this.scorer = scorer
    this.subScorers = CombinedIndex.extractSubScorers(scorer)
  }

  override def acceptsDocsOutOfOrder: Boolean = false

  override def collect(doc: Int): Unit = {
    this.scorer.advance(doc)
    val score = this.scorer.score
    val subScores = this.subScorers.map(_.score)
    priorityQueue.insertWithOverflow(DocScores(doc, score, subScores))
  }

  override def setNextReader(context: AtomicReaderContext): Unit = {}

  def getDocSubScores: Seq[DocScores] = {
    val buffer = Buffer.empty[DocScores]
    while (this.priorityQueue.size > 0) {
      buffer += this.priorityQueue.pop
    }
    buffer
  }
}
