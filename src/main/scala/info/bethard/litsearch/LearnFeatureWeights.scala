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
import org.apache.lucene.index.Term
import org.apache.lucene.search.BooleanClause
import org.apache.lucene.search.BooleanQuery
import org.apache.lucene.search.Collector
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.NumericRangeQuery
import org.apache.lucene.search.Scorer
import org.apache.lucene.search.TermQuery
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

    @CliOption(longName = Array("indexes"))
    def getIndexFiles: java.util.List[File]

    @CliOption(longName = Array("svm-map-dir"))
    def getSvmMapDir: File

    @CliOption(longName = Array("svm-map-cost"), defaultValue = Array("1000"))
    def getSvmMapCost: Int

    @CliOption(longName = Array("n-hits"), defaultValue = Array("100"))
    def getNHits: Int

    @CliOption(longName = Array("n-iterations"), defaultValue = Array("10"))
    def getNIterations: Int

    @CliOption(longName = Array("accession-numbers"), defaultValue = Array(
        // Adverse birth outcomes in African American women: the social context of persistent reproductive disadvantage
        "000296271400002", 
        // Psychological science on pregnancy: stress processes, biopsychosocial models, and emerging research issues
        "000287331200020", 
        // Intention to become pregnant and low birth weight and preterm birth: a systematic review
        "000286603900008", 
        // Linkages among reproductive health, maternal health, and perinatal outcomes
        "000285527800008", 
        // Baby on board: do responses to stress in the maternal brain mediate adverse pregnancy outcome?
        "000280535500009", 
        // Not available: Global report on preterm birth and stillbirth (2 of 7): discovery science.
        // Disasters and perinatal health:a systematic review
        "000288002900013",
        // Support during pregnancy for women at increased risk of low birthweight babies
        "000278858300045", 
        //Maternal exposure to domestic violence and pregnancy and birth outcomes: a systematic review and meta-analyses
        "000283855500011",
        //The role of stress in female reproduction and pregnancy: an update
        "000283095800011",
        //Hurricane Katrina and perinatal health
        "000271972900008",
        //What causes racial disparities in very preterm birth? A biosocial perspective
        "000271814500006",
        //The interaction between chronic stress and pregnancy: preterm birth from a biobehavioral perspective
        "000262293200003",
        //Race, racism, and racial disparities in adverse birth outcomes
        "000256184000017",
        //Psychosocial stress and pregnancy outcome
        "000256184000015",
        //Spontaneous preterm birth, a clinical dilemma: etiologic, pathophysiologic and genetic heterogeneities and racial disparity
        "000257910700002",
        //Depression and anxiety during pregnancy: a risk factor for obstetric, fetal and neonatal outcome? A critical review of the literature
        "000245588000001",
        //The interrelationship of maternal stress, endocrine factors and inflammation on gestational length
        "000183691600004"
        // Not available in WoS: Lowering the premature birth rate: what the U.S. experience means for Japan
        ))
    def getArticleAccessionNumbers: java.util.List[String]
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

    // scale to [0, 500] since that's about the range that the text queries return
    val logThenScale = QueryFunctions.logOf1Plus andThen QueryFunctions.scaleBetween(0f, 500f)

    // create indexes from command line parameters
    val textIndex = new TitleAbstractTextIndex
    val citationCountIndex = new CitationCountIndex(logThenScale)
    val ageIndex = new AgeIndex(2012, logThenScale)

    // open the reader and searcher
    val indexFiles = options.getIndexFiles.asScala
    val subReaders = for (f <- indexFiles) yield DirectoryReader.open(FSDirectory.open(f))
    val reader = new ParallelCompositeReader(subReaders: _*)
    val searcher = new IndexSearcher(reader)

    // function for finding articles based on accession number (WoK article ID)
    val getDocID: String => Option[Int] = articleID => {
      val query = new TermQuery(new Term(IndexConfig.FieldNames.articleID, articleID))
      val topDocs = searcher.search(query, 2)
      if (topDocs.scoreDocs.length < 1) {
        System.err.println("WARNING: no article for id " + articleID)
        None
      } else if (topDocs.scoreDocs.length > 1) {
        System.err.println("WARNING: more than one article for id " + articleID)
        None
      } else {
        Some(topDocs.scoreDocs(0).doc)
      }
    }
    
    // determine the documents for training
    val articleIDs = options.getArticleAccessionNumbers.asScala
    val docs = for (articleID <- articleIDs; docID <- getDocID(articleID)) yield docID 
    this.logger.info("Found %d documents".format(docs.size))

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
        citedIdsString <- Option(queryDocument.get(FieldNames.citedArticleIDs)).iterator
        abstractText = Option(queryDocument.get(FieldNames.abstractText)).getOrElse("")
        titleText = Option(queryDocument.get(FieldNames.titleText)).getOrElse("")
        text = abstractText + titleText
        if !text.isEmpty
      } yield {
        val year = queryDocument.get(FieldNames.year).toInt

        // determine what articles were cited
        val citedIds = citedIdsString.split("\\s+").toSet

        // create the query based from the title and abstract text,
        // and only considering articles published before the query article
        val query = new BooleanQuery()
        val publishedBefore = NumericRangeQuery.newIntRange(
            FieldNames.year, null, year, true, false)
        query.add(publishedBefore, BooleanClause.Occur.MUST)
        query.add(index.createQuery(text), BooleanClause.Occur.MUST)
        println(query)

        // retrieve other articles based on the main query
        val collector = new DocScoresCollector(nHits)
        searcher.search(query, collector)
        val docSubScores = collector.popDocSubScores
        val results = for (docScores <- docSubScores) yield {
          val resultDocument = reader.document(docScores.doc)
          val id = resultDocument.get(FieldNames.articleIDWhenCited)

          // generate SVM-style label and feature string
          val label = if (citedIds.contains(id)) "+1" else "-1"
          (label, doc, docScores.subScores)
        }
        val labels = results.map(_._1)

        // only add the file if both positive and negative examples were found
        if (labels.toSet.size == 2) {
          nWritten += 1
          
          // write the file for this document
          val trainingDataFile = new File(trainingDataDir, doc + ".svmmap")
          val trainingDataWriter = new PrintWriter(trainingDataFile)
          for ((label, doc, subScores) <- results) {
            val featuresString = featureFormat.format(subScores: _*)
            trainingDataWriter.println(lineFormat.format(label, doc, featuresString))
          }
          trainingDataWriter.close
          
          // add the file path to the list of data files
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
      val maxWeight = weights.map(math.abs).max
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

  def popDocSubScores: Seq[DocScores] = {
    val buffer = Buffer.empty[DocScores]
    while (this.priorityQueue.size > 0) {
      buffer += this.priorityQueue.pop
    }
    buffer.reverse
  }
}
