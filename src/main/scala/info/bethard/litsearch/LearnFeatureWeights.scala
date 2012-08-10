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
import scala.sys.process.Process
import info.bethard.litsearch.IndexConfig.FieldNames
import scala.sys.process.ProcessIO
import java.util.logging.Logger

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

    @CliOption(longName = Array("n-iterations"), defaultValue = Array("10"))
    def getNIterations: Int

    @CliOption(longName = Array("svm-map-dir"))
    def getSvmMapDir: File
  }

  def main(args: Array[String]): Unit = {
    val options = CliFactory.parseArguments(classOf[Options], args: _*)
    val nHits = options.getNHits
    val modelDir = options.getModelDir

    // determine names for files in the model directory
    if (!modelDir.exists) {
      modelDir.mkdirs
    }
    val trainingDataIndexFile = new File(options.getModelDir, "training-data.svmmap-index")
    val trainingDataDir = new File(options.getModelDir, "training-data")
    if (!trainingDataDir.exists) {
      trainingDataDir.mkdir
    }

    // create indexes from command line parameters
    val titleIndex = new TitleTextIndex
    val abstractIndex = new AbstractTextIndex
    val citationCountIndex = new CitationCountIndex
    val ageIndex = new AgeIndex(2012)

    // create main index as weighted sum of other indexes 
    val indexes = Seq(titleIndex, abstractIndex, citationCountIndex, ageIndex)
    var weights = Seq(1f, 1f, 0f, 0f)
    for (i <- 1 to options.getNIterations) {
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
      val featureFormat = (0 until weights.size).map(_ + ":%f").mkString(" ")
      val lineFormat = "%s qid:%d %s"

      // generate one training example for each document in the index
      val trainingDataIndexWriter = new PrintWriter(trainingDataIndexFile)
      val averagePrecisions = for {
        doc <- 0 until reader.maxDoc
        queryDocument = reader.document(doc)
        // only use articles with an abstract and a bibliography
        citedIdsString <- Option(queryDocument.get(FieldNames.citedArticleIDs))
        abstractText <- Option(queryDocument.get(FieldNames.abstractText))
      } yield {

        // prepare to write SVM-MAP files
        val trainingDataFile = new File(trainingDataDir, doc + ".svmmap")
        val trainingDataWriter = new PrintWriter(trainingDataFile)

        // determine what articles were cited
        val citedIds = citedIdsString.split("\\s+").toSet

        // create the query based on the abstract text
        val query = index.createQuery(abstractText)

        // retrieve other articles based on the main query
        val collector = new DocScoresCollector(nHits)
        searcher.search(query, collector)
        val labels = for (docScores <- collector.getDocSubScores) yield {
          val resultDocument = reader.document(docScores.doc)
          val id = resultDocument.get(FieldNames.articleIDWhenCited)

          // generate SVM-style label and feature string
          val label = if (citedIds.contains(id)) "+1" else "-1"
          val featuresString = featureFormat.format(docScores.subScores: _*)
          trainingDataWriter.println(lineFormat.format(label, doc, featuresString))
          label
        }

        // close the writer
        trainingDataWriter.close
        if ((doc + 1) % 100 == 0) {
          this.logger.info("Wrote training data for %d articles".format(doc + 1))
        }

        // only add the file for training if there are both positive and negative examples
        if (labels.toSet.size == 2) {
          trainingDataIndexWriter.println(trainingDataFile.getAbsolutePath)
          trainingDataIndexWriter.flush
        }

        // calculate and yield the average precision
        val relevantRanks =
          for ((label, rank) <- labels.zipWithIndex; if label == "+1") yield rank
        val precisions =
          for ((rank, index) <- relevantRanks.zipWithIndex) yield (1.0 + index) / (1.0 + rank)
        precisions.sum / citedIds.size
      }
      // close the reader and index writer
      reader.close
      trainingDataIndexWriter.close

      // log the mean average precision
      this.logger.info("MAP: %.3f".format(averagePrecisions.sum / averagePrecisions.size))

      // train the model
      val modelFile = new File(options.getModelDir, "model.svmmap")
      val svmMapPath = new File(options.getSvmMapDir, "svm_map_learn").getPath
      val svmMapCommand = Seq(svmMapPath, "-c", "1000", trainingDataIndexFile.getPath, modelFile.getPath)
      val svmMap = Process(svmMapCommand, None, "PYTHONPATH" -> options.getSvmMapDir.getPath)
      svmMap.!

      val printWeightsCommand = """import pickle; """ +
        """SVMBlank = type("SVMBlank", (), {}); """ +
        """model = pickle.load(open("%s")); """.format(modelFile) +
        """print " ".join(map(str, model.w))"""
      val output = Process(Seq("python", "-c", printWeightsCommand)).lines.mkString
      weights = output.split("\\s+").map(_.toFloat).toSeq
      this.logger.info("Current weights: " + weights)
    }
  }

  val logger = Logger.getLogger(this.getClass.getName)
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
