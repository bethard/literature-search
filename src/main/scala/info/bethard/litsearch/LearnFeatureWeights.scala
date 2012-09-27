package info.bethard.litsearch

import java.io.File
import java.io.FileWriter
import java.io.PrintWriter
import java.util.logging.Logger

import scala.collection.JavaConverters._
import scala.collection.mutable.Buffer
import scala.sys.process.Process

import org.apache.lucene.index.AtomicReaderContext
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.index.ParallelCompositeReader
import org.apache.lucene.search.BooleanClause
import org.apache.lucene.search.BooleanQuery
import org.apache.lucene.search.Collector
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.Scorer
import org.apache.lucene.search.TermRangeQuery
import org.apache.lucene.search.similarities.DefaultSimilarity
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.util.PriorityQueue

import com.google.common.io.Files
import com.lexicalscope.jewel.cli.CliFactory
import com.lexicalscope.jewel.cli.{ Option => CliOption }

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

    @CliOption(longName = Array("n-iterations"), defaultValue = Array("10"))
    def getNIterations: Int

    @CliOption(longName = Array("max-articles"), defaultValue = Array("100"))
    def getMaxArticles: Int

    @CliOption(longName = Array("min-article-references"), defaultValue = Array("20"))
    def getMinArticleReferences: Int

    @CliOption(longName = Array("min-article-citation-count"), defaultValue = Array("20"))
    def getMinArticleCitationCount: Int

    @CliOption(longName = Array("svm-map-dir"))
    def getSvmMapDir: File

    @CliOption(longName = Array("svm-map-cost"), defaultValue = Array("1"))
    def getSvmMapCost: Int

    @CliOption(longName = Array("query"), defaultValue = Array("premature"))
    def getQuery: String
  }

  def main(args: Array[String]): Unit = {
    val options = CliFactory.parseArguments(classOf[Options], args: _*)
    val nHits = options.getNHits
    val modelDir = options.getModelDir

    // determine names for files in the model directory
    if (!modelDir.exists) {
      modelDir.mkdirs
    }
    val getTrainingDataIndexFile = (iteration: Int) =>
      new File(options.getModelDir, "training-data-%d.svmmap-index".format(iteration))

    // create indexes from command line parameters
    val textIndex = new TitleAbstractTextIndex
    val citationCountIndex = new CitationCountIndex
    val ageIndex = new AgeIndex(2012)

    // open the reader and searcher
    val articlesReader = DirectoryReader.open(FSDirectory.open(options.getArticleIndex))
    val citationCountReader = DirectoryReader.open(FSDirectory.open(options.getCitationCountIndex))
    val reader = new ParallelCompositeReader(articlesReader, citationCountReader)
    val searcher = new IndexSearcher(reader)

    // determine the documents for training
    val queryString = options.getQuery
    val topDocs = searcher.search(textIndex.createQuery(queryString), reader.maxDoc)
    val docs = topDocs.scoreDocs.iterator.map(_.doc).filter { docID =>
      val document = reader.document(docID)
      Option(document.get(FieldNames.citedArticleIDs)) match {
        case None => false
        case Some(citedIDsString) => {
          val hasReferences = citedIDsString.split("\\s+").toSet.size >= options.getMinArticleReferences
          val hasCitations = document.get(FieldNames.citationCount).toInt >= options.getMinArticleCitationCount
          hasReferences && hasCitations
        }
      }
    }.take(options.getMaxArticles).toSeq
    this.logger.info("Found %d documents containing \"%s\"".format(docs.size, queryString))

    // remove any existing training data files
    for (iteration <- 1 to options.getNIterations) {
      val trainingDataIndexFile = getTrainingDataIndexFile(iteration)
      if (trainingDataIndexFile.exists()) {
        trainingDataIndexFile.delete()
      }
    }

    // initialize index weights
    val indexes = Seq(textIndex, citationCountIndex, ageIndex)
    var weights = Seq(1f, 0f, 0f)

    // prepare for writing output
    val featureFormat = (0 until weights.size).map(_ + ":%f").mkString(" ")
    val lineFormat = "%s qid:%d %s"
    for (iteration <- 1 to options.getNIterations) {
      this.logger.info("Current weights: " + weights)

      // create main index as weighted sum of other indexes 
      val index = new CombinedIndex(indexes zip weights: _*)

      //get rid of the query norm so that scores are directly interpretable
      searcher.setSimilarity(new DefaultSimilarity {
        override def queryNorm(sumOfSquaredWeights: Float) = 1f
      })

      // prepare directory for svm files
      val trainingDataDir = new File(options.getModelDir, "training-data-" + iteration)
      if (!trainingDataDir.exists) {
        trainingDataDir.mkdir
      }

      // prepare index of svm files (starting with index from previous iteration)
      val trainingDataIndexFile = getTrainingDataIndexFile(iteration)
      if (iteration > 1) {
        Files.copy(getTrainingDataIndexFile(iteration - 1), trainingDataIndexFile)
      }

      // generate one training example for each document in the index
      var nWritten = 0
      val averagePrecisions = for {
        doc <- docs
        queryDocument = reader.document(doc)
        // only use articles with an abstract and a bibliography
        citedIdsString <- Option(queryDocument.get(FieldNames.citedArticleIDs))
        abstractText = Option(queryDocument.get(FieldNames.abstractText)).getOrElse("")
        titleText = Option(queryDocument.get(FieldNames.titleText)).getOrElse("")
        text = abstractText + titleText
        if !text.isEmpty
      } yield {
        val year = queryDocument.get(FieldNames.year).toInt

        // prepare to write SVM-MAP files
        val trainingDataFile = new File(trainingDataDir, doc + ".svmmap")
        val trainingDataWriter = new PrintWriter(trainingDataFile)

        // determine what articles were cited
        val citedIds = citedIdsString.split("\\s+").toSet

        // create the query based from the title and abstract text,
        // and only considering articles published before the query article
        val query = new BooleanQuery()
        val publishedBefore = TermRangeQuery.newStringRange(
          FieldNames.year, (year - 100).toString, year.toString, true, false)
        query.add(publishedBefore, BooleanClause.Occur.MUST)
        query.add(index.createQuery(text), BooleanClause.Occur.MUST)
        println(query)

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

        // only add the file if both positive and negative examples were found
        if (labels.toSet.size == 2) {
          nWritten += 1
          val trainingDataIndexWriter = new FileWriter(trainingDataIndexFile, true)
          trainingDataIndexWriter.write(trainingDataFile.getAbsolutePath)
          trainingDataIndexWriter.write('\n')
          trainingDataIndexWriter.close
        }

        // calculate and yield the average precision
        val relevantRanks =
          for ((label, rank) <- labels.zipWithIndex; if label == "+1") yield rank
        val precisions =
          for ((rank, index) <- relevantRanks.zipWithIndex) yield (1.0 + index) / (1.0 + rank)
        precisions.sum / citedIds.size
      }

      // log the mean average precision
      this.logger.info("MAP: %.4f".format(averagePrecisions.sum / averagePrecisions.size))
      if (nWritten == 0) {
        throw new Exception("No new training examples found")
      }
      this.logger.info("Added %d training examples".format(nWritten))

      // train the model
      val modelFile = new File(options.getModelDir, "model-%d.svmmap".format(iteration))
      val svmMapPath = new File(options.getSvmMapDir, "svm_map_learn").getPath
      val cost = options.getSvmMapCost.toString
      val svmMapCommand = Seq(svmMapPath, "-c", cost, trainingDataIndexFile.getPath, modelFile.getPath)
      val svmMap = Process(svmMapCommand, None, "PYTHONPATH" -> options.getSvmMapDir.getPath)
      svmMap.!

      val printWeightsCommand = """import pickle; """ +
        """SVMBlank = type("SVMBlank", (), {}); """ +
        """model = pickle.load(open("%s")); """.format(modelFile) +
        """print " ".join(map(str, model.w))"""
      val output = Process(Seq("python", "-c", printWeightsCommand)).lines.mkString
      weights = output.split("\\s+").map(_.toFloat).toSeq
      val maxWeight = weights.max
      weights = weights.map(_ / maxWeight)
    }
    this.logger.info("Final weights: " + weights)

    // close the reader
    reader.close
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
    // get scorer without year restriction, then get sub-scorers from combined index
    val Seq(yearScorer, queryScorer) = scorer.getChildren.asScala.toSeq
    this.subScorers = CombinedIndex.extractSubScorers(queryScorer.child)
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
    buffer.reverse
  }
}
