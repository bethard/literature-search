package info.bethard.litsearch

import java.io.File
import java.util.logging.Logger

import scala.collection.JavaConverters._
import scala.collection.mutable.Buffer

import org.apache.lucene.index.AtomicReaderContext
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.index.IndexReader
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

import com.lexicalscope.jewel.cli.CliFactory
import com.lexicalscope.jewel.cli.{ Option => CliOption }

import de.bwaldvogel.liblinear.Feature
import de.bwaldvogel.liblinear.FeatureNode
import de.bwaldvogel.liblinear.Linear
import de.bwaldvogel.liblinear.Parameter
import de.bwaldvogel.liblinear.Problem
import de.bwaldvogel.liblinear.SolverType

import info.bethard.litsearch.IndexConfig.FieldNames

object LearnFeatureWeights {

  val logger = Logger.getLogger(this.getClass.getName)

  trait Options {
    @CliOption(longName = Array("indexes"))
    def getIndexFiles: java.util.List[File]

    @CliOption(longName = Array("n-hits"), defaultValue = Array("1000"))
    def getNHits: Int

    @CliOption(longName = Array("n-iterations"), defaultValue = Array("5"))
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
    val articleIDs = options.getArticleAccessionNumbers.asScala

    // create indexes from command line parameters
    val textIndex = new TitleAbstractTextIndex
    val citationCountIndex = new CitationCountIndex(identity)
    val ageIndex = new AgeIndex(2012, identity)
    val indexes = List[Index](textIndex, citationCountIndex, ageIndex)

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
        this.logger.warning("no article for id " + articleID)
        None
      } else if (topDocs.scoreDocs.length > 1) {
        this.logger.warning("WARNING: more than one article for id " + articleID)
        None
      } else {
        Some(topDocs.scoreDocs(0).doc)
      }
    }

    // determine the documents for training
    val docIDs = for (articleID <- articleIDs; docID <- getDocID(articleID)) yield docID
    this.logger.info("Found %d documents".format(docIDs.size))

    val scoredWeightsIter = new LearnFeatureWeightsIterator(
      indexes,
      List(1.0f, 0.0f, 0.0f),
      reader,
      searcher,
      docIDs,
      options.getNHits)

    val scoredWeights = Buffer.empty[(Seq[Float], Double)]
    for ((weights, map) <- scoredWeightsIter.take(options.getNIterations)) {
      this.logger.info("%.4f MAP for %s".format(map, weights))
      scoredWeights += weights -> map
    }
    val weights = scoredWeights.drop(2).maxBy(_._2)._1
    this.logger.info("Final weights: " + weights.toList)

    reader.close
  }
}

class LearnFeatureWeightsIterator(
    indexes: Seq[Index],
    initialWeights: Seq[Float],
    reader: IndexReader,
    searcher: IndexSearcher,
    docIDs: Seq[Int],
    nHits: Int) extends Iterator[(Seq[Float], Double)] {
  
  // assemble just the minimum information we need from the training articles
  val articles = for {
    docID <- docIDs
    queryDocument = reader.document(docID)
    citedIdsString <- Option(queryDocument.get(FieldNames.citedArticleIDs)).iterator
  } yield {
    val year = queryDocument.get(FieldNames.year).toInt
    val abstractText = Option(queryDocument.get(FieldNames.abstractText)).getOrElse("")
    val titleText = Option(queryDocument.get(FieldNames.titleText)).getOrElse("")
    val citedIDs = citedIdsString.split("\\s+").toSet
    (docID, abstractText + titleText, year, citedIDs)
  }

  // get rid of the query norm so that scores are directly interpretable
  searcher.setSimilarity(new DefaultSimilarity {
    override def queryNorm(sumOfSquaredWeights: Float) = 1f
  })

  // initialize index weights
  var weights = initialWeights
  var index = new CombinedIndex(this.indexes zip this.weights: _*)

  // create training instance buffers; add a fake instance to guarantee that label +1 is index 1
  val instanceFeatures = Buffer[Array[Feature]](Array.empty)
  val instanceLabels = Buffer[Double](0.0)

  // infinite iterator, always return true
  def hasNext = true

  // score the current weights, and advance to the next weights
  def next: (Seq[Float], Double) = {
    val averagePrecisions = this.articles.map(this.scoreRetrievalAndAddInstances)
    val result = (this.weights, averagePrecisions.sum / averagePrecisions.size)
    this.weights = this.trainWeights
    this.index = new CombinedIndex(this.indexes zip this.weights: _*)
    result
  }

  // run a retrieval using the current weights and calculate mean average precision
  // at the same time, collect retrieved articles as new training instances
  def scoreRetrievalAndAddInstances(article: (Int, String, Int, Set[String])): Double = {
    val (docID, text, year, citedIDs) = article

    // create the query, only considering articles published before the query article
    val query = new BooleanQuery()
    val publishedBefore = NumericRangeQuery.newIntRange(FieldNames.year, null, year, true, false)
    query.add(publishedBefore, BooleanClause.Occur.MUST)
    query.add(this.index.createQuery(text), BooleanClause.Occur.MUST)

    // retrieve other articles based on the main query
    val collector = new DocScoresCollector(nHits)
    searcher.search(query, collector)
    val docSubScores = collector.popDocSubScores
    val labels = for (docScores <- docSubScores) yield {
      val resultDocument = reader.document(docScores.doc)
      val id = resultDocument.get(FieldNames.articleIDWhenCited)

      // add label and features to training data
      val label = citedIDs.contains(id)
      val features =
        for ((score, i) <- docScores.subScores.zipWithIndex)
          yield new FeatureNode(i + 1, score)
      instanceFeatures += features.toArray
      instanceLabels += (if (label) 1.0 else 0.0)
      label
    }

    // calculate and yield the average precision
    val relevantRanks =
      for ((label, rank) <- labels.zipWithIndex; if label) yield rank
    val precisions =
      for ((rank, index) <- relevantRanks.zipWithIndex) yield (1.0 + index) / (1.0 + rank)
    precisions.sum / citedIDs.size
  }

  // train a new set of weights from the training instances collected so far
  def trainWeights: Seq[Float] = {
    
    // define the SVM problem from the training instances
    val problem = new Problem
    problem.x = this.instanceFeatures.toArray
    problem.y = this.instanceLabels.toArray
    problem.l = problem.x.length
    problem.n = this.initialWeights.length
    
    // set the SVM hyper-parameters
    val solver = SolverType.L2R_L2LOSS_SVC
    val C = 10000.0
    val eps = 0.001
    val weightForPositiveExamples = 1000.0
    val parameters = new Parameter(solver, C, eps)
    parameters.setWeights(Array(weightForPositiveExamples), Array(1))
    
    // train the SVM model
    val model = Linear.train(problem, parameters)
    
    // get the SVM weights, recalling that positive weights predict class 0 (we want the reverse)
    val weights = model.getFeatureWeights.map(-_.toFloat)
    
    // normalize weights so that the largest weight is 1.0
    val maxWeight = weights.map(math.abs).max
    weights.map(_ / maxWeight)
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
