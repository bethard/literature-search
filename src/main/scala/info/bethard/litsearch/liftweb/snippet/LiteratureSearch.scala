package info.bethard.litsearch.liftweb.snippet

import scala.xml.NodeSeq

import org.apache.lucene.search.TopDocs

import info.bethard.litsearch.AgeIndex
import info.bethard.litsearch.CitationCountIndex
import info.bethard.litsearch.CombinedIndex
import info.bethard.litsearch.TitleAbstractTextIndex
import net.liftweb.http.S
import net.liftweb.http.SHtml
import net.liftweb.http.SessionVar
import net.liftweb.util.AnyVar.whatVarIs
import net.liftweb.util.Helpers.strToCssBindPromoter
import net.liftweb.util.Helpers.tryo

object LiteratureSearch {
  
  val textIndex = new TitleAbstractTextIndex
  val citationCountIndex = new CitationCountIndex(identity)
  val ageIndex = new AgeIndex(2012, identity)

  val citationCountScalingFactor = 50.0f
  val ageScalingFactor = 2.0f
  
  private val learnedTextWeight = 1.0f
  private val learnedCitationCountWeight = 0.0044f * citationCountScalingFactor
  private val learnedAgeWeight = -0.09 * ageScalingFactor
  
  private val learnedTextWeightString = "%.2f".format(learnedTextWeight)
  private val learnedCitationCountWeightString = "%.2f".format(learnedCitationCountWeight)
  private val learnedAgeWeightString = "%.2f".format(learnedAgeWeight)
  
  private class SearchResettingSessionVar[T](value: T) extends SessionVar(value){
    override def set(value: T) = {
      if (value != this.is) {
        nHits.set(10)
        results.set(None)
      }
      super.set(value)
    }
  }

  private object query extends SearchResettingSessionVar("")
  private object results extends SessionVar[Option[TopDocs]](None)

  private object nHits extends SessionVar(10)
  private object textWeight extends SearchResettingSessionVar(learnedTextWeightString)
  private object citationCountWeight extends SearchResettingSessionVar(learnedCitationCountWeightString)
  private object ageWeight extends SearchResettingSessionVar(learnedAgeWeightString)

  def render = {

    // verify input values, and then return JavaScript that will post the results 
    def process(): Unit = {
      val stringWeightTuples = Seq(
        (textWeight, "text"),
        (citationCountWeight, "citation-count"),
        (ageWeight, "age"))

      // issue errors for any weights that aren't numbers
      for ((weight, name) <- stringWeightTuples; if tryo(weight.is.toFloat).isEmpty) {
        S.error(name + "-error", "The " + name + " weight must be a number")
      }

      // use the weights to get the results 
      for {
        textWeight <- tryo(textWeight.is.toFloat)
        citationCountWeight <- tryo(citationCountWeight.is.toFloat)
        ageWeight <- tryo(ageWeight.is.toFloat)
        if !query.isEmpty
      } {

        // create the index with the given weights
        val index = new CombinedIndex(
          this.textIndex -> textWeight,
          this.citationCountIndex -> citationCountWeight / this.citationCountScalingFactor,
          this.ageIndex -> ageWeight / this.ageScalingFactor)

        // search for the given query
        val topDocs = Search.searcher.search(index.createQuery(query), nHits.is)
        results.set(Some(topDocs))
      }
    }

    // set default input element values and register actions for storing modified values
    "#results" #> results.is.map(this.renderResults).getOrElse(NodeSeq.Empty) &
      "name=query" #> SHtml.textElem(query) &
      "name=weight-text" #> SHtml.textElem(textWeight) &
      "name=weight-citation-count" #> SHtml.textElem(citationCountWeight) &
      "name=weight-age" #> SHtml.textElem(ageWeight) &
      "#more" #> {
        if (results.is.isEmpty) SHtml.hidden(() => ())
        else SHtml.submit("More", () => { nHits.update(_ + 10); process })
      } &
      "#submit" #> SHtml.onSubmitUnit(process)
  }
  
  def renderResults(topDocs: TopDocs): NodeSeq = {
    val articleElemIter = topDocs.scoreDocs.iterator.map(_.doc).map(Search.renderArticle)
    val foundHits = <div>Found { topDocs.totalHits } results</div>
    val articles = <ol>{ articleElemIter.map(elem => <li>{ elem }</li>) }</ol>
    val resultsDiv = <div>{ foundHits ++ articles }</div>
    resultsDiv
  }
}
