package info.bethard.litsearch.liftweb.snippet

import scala.collection.mutable
import scala.xml.Elem
import scala.xml.NodeSeq

import org.apache.lucene.search.TopDocs

import info.bethard.litsearch.AgeIndex
import info.bethard.litsearch.CitationCountIndex
import info.bethard.litsearch.LearnFeatureWeights
import info.bethard.litsearch.TitleAbstractTextIndex
import net.liftweb.http.SHtml
import net.liftweb.http.SessionVar
import net.liftweb.util.Helpers.strToCssBindPromoter
import net.liftweb.util.PassThru

object Train {

  val indexes = List(
    Search.textIndex,
    Search.citationCountIndex,
    Search.ageIndex)

  val weightNames = List(
    "Text weight",
    "Citation count weight",
    "Age weight")

  val weightMultipliers = List(
    1.0,
    Search.citationCountScalingFactor,
    Search.ageScalingFactor)

  private object query extends SessionVar("")
  private object nHits extends SessionVar(10)
  private object results extends SessionVar[Option[TopDocs]](None)
  private object trainingDocs extends SessionVar(mutable.LinkedHashSet.empty[Int])
  private object weights extends SessionVar[Option[Seq[Float]]](None)

  def render = {
    "name=query" #> SHtml.textElem(this.query) andThen
      "#search-results" #> this.renderResults.getOrElse(NodeSeq.Empty) andThen
      "#submit" #> SHtml.onSubmitUnit(() => this.queryIndex) andThen
      "name=add-article" #> SHtml.onSubmit(s => addTrainingArticle(s.toInt)) andThen
      "#no-articles-selected" #> this.hideIf(this.trainingDocs.is.size > 0) andThen 
      "#articles-selected" #> this.renderArticlesSelected andThen
      "name=remove-article" #> SHtml.onSubmit(s => removeTrainingArticle(s.toInt)) andThen
      "#no-training-allowed" #> this.hideIf(this.trainingDocs.is.size >= 10) andThen
      "#training-allowed" #> this.hideIf(this.trainingDocs.is.size < 10) andThen
      "#train" #> SHtml.onSubmitUnit(() => this.train) andThen
      "#model-recommendations" #> this.hideIf(this.weights.is.isEmpty) andThen
      "#model-weights" #> this.renderModelWeights.getOrElse(NodeSeq.Empty)
  }

  def queryIndex: Unit = {
    if (!this.query.is.isEmpty) {
      val indexQuery = Search.textIndex.createQuery(this.query.is)
      val topDocs = Search.searcher.search(indexQuery, nHits.is)
      this.results.set(Some(topDocs))
    }
  }

  def renderResults: Option[Elem] = {
    for (topDocs <- this.results.is) yield {
      val docIDs = topDocs.scoreDocs.iterator.map(_.doc).filterNot(this.trainingDocs.is)
      val articleItems = for (docID <- docIDs) yield {
        val elem = Search.renderArticle(docID)
        <p><button name="add-article" value={ docID.toString }>Use for training</button>{ elem }</p>
      }
      val foundHits = <div>Found { topDocs.totalHits } results</div>
      val resultsDiv = <div>{ foundHits ++ articleItems }</div>
      resultsDiv
    }
  }

  def addTrainingArticle(docID: Int): Unit = {
    this.trainingDocs.is += docID
    this.weights.set(None)
  }

  def removeTrainingArticle(docID: Int): Unit = {
    this.trainingDocs.is -= docID
    this.weights.set(None)
  }
  
  def renderArticlesSelected: Elem = {
    val articleItems = for (docID <- this.trainingDocs.is.iterator) yield {
      <p><button name="remove-article" value={ docID.toString }>Remove from training</button>{ Search.renderArticle(docID) }</p>
    }
    <div>{ articleItems }</div>
  }
  
  def hideIf(condition: Boolean): NodeSeq => NodeSeq = {
    if (condition) {
      "* [style]" #> "display:none" 
    } else {
      PassThru
    }
  }
  
  def renderModelWeights: Option[Elem] = {
    for (weights <- this.weights.is) yield {
      val adjustedWeights = (weights, this.weightMultipliers).zipped.map(_ * _)
      val weightElems = for ((name, weight) <- this.weightNames zip adjustedWeights) yield {
        <li>{ name }: { "%.2f".format(weight) }</li>
      }
      <ul>{ weightElems }</ul>
    }
  }

  def train: Unit = {
    val textIndex = new TitleAbstractTextIndex
    val citationCountIndex = new CitationCountIndex(identity)
    val ageIndex = new AgeIndex(2012, identity)
    val nHits = 1000
    val nIterations = 5
    val weights = LearnFeatureWeights.learnWeights(
      this.indexes,
      List(1.0f, 0.0f, 0.0f),
      Search.reader,
      Search.searcher,
      this.trainingDocs.is.toSeq,
      nHits, nIterations)
    this.weights.set(Some(weights))
    println(this.weights.is)
  }
}
