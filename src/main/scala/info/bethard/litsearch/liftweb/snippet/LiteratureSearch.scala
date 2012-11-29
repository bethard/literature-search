package info.bethard.litsearch.liftweb.snippet

import java.io.File

import scala.xml.NodeSeq
import scala.xml.Text

import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.index.ParallelCompositeReader
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.TopDocs
import org.apache.lucene.store.FSDirectory

import info.bethard.litsearch.AgeIndex
import info.bethard.litsearch.CitationCountIndex
import info.bethard.litsearch.CombinedIndex
import info.bethard.litsearch.IndexConfig
import info.bethard.litsearch.QueryFunctions
import info.bethard.litsearch.TitleAbstractTextIndex
import net.liftweb.common.Box
import net.liftweb.common.Box.option2Box
import net.liftweb.common.Empty
import net.liftweb.http.S
import net.liftweb.http.SHtml
import net.liftweb.http.SessionVar
import net.liftweb.util.AnyVar.whatVarIs
import net.liftweb.util.Helpers.strToCssBindPromoter
import net.liftweb.util.Helpers.tryo
import net.liftweb.util.Props

object LiteratureSearch {
  
  private val citationCountScalingFactor = 50.0f
  private val ageScalingFactor = 2.0f
  
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
        results.set(Empty)
      }
      super.set(value)
    }
  }

  private object query extends SearchResettingSessionVar("")
  private object results extends SessionVar[Box[TopDocs]](Empty)

  private object nHits extends SessionVar(10)
  private object textWeight extends SearchResettingSessionVar(learnedTextWeightString)
  private object citationCountWeight extends SearchResettingSessionVar(learnedCitationCountWeightString)
  private object ageWeight extends SearchResettingSessionVar(learnedAgeWeightString)

  val wokURLBase = "http://apps.webofknowledge.com/InboundService.do?product=WOS&action=retrieve&mode=FullRecord&UT="

  val textIndex = new TitleAbstractTextIndex
  val citationCountIndex = new CitationCountIndex(identity)
  val ageIndex = new AgeIndex(2012, identity)

  // open readers to all the indexes
  val reader = {
    val pathsString = Props.get("indexes").failMsg("\"indexes\" property missing").open_!
    val indexReaders = for (indexPath <- pathsString.split(",")) yield {
      DirectoryReader.open(FSDirectory.open(new File(indexPath)))
    }
    new ParallelCompositeReader(indexReaders: _*)  
  }
  
  // opent the searcher
  val searcher = new IndexSearcher(reader)

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
        val topDocs = this.searcher.search(index.createQuery(query), nHits.is)
        results.set(Some(topDocs))
      }
    }

    // set default input element values and register actions for storing modified values
    "#results" #> results.is.map(this.renderResults).openOr(NodeSeq.Empty) &
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
    // convert document results to HTML
    val articleItems = for (scoreDoc <- topDocs.scoreDocs.iterator) yield {
      val doc = reader.document(scoreDoc.doc)
      val wokID = doc.get(IndexConfig.FieldNames.articleID)
      val title = doc.get(IndexConfig.FieldNames.titleText)
      val source = doc.get(IndexConfig.FieldNames.sourceTitleText)
      val year = doc.get(IndexConfig.FieldNames.year)
      val authors = Option(doc.get(IndexConfig.FieldNames.authors)) match {
        case None => Array.empty[String]
        case Some(string) => for (authorString <- string.split(" ")) yield {
          authorString.split("_+") match {
            case Array(lastName) => lastName.head.toUpper + lastName.tail
            case Array(lastName, initials) => {
              val parts = initials.map(_.toUpper + ".") :+ (lastName.head.toUpper + lastName.tail)
              parts.mkString(" ")
            }
            case _ => {
              System.err.printf("Unexpected author format: \"%s\"\n", authorString)
              authorString
            }
          }
        }
      }
      val citationCount = doc.get(IndexConfig.FieldNames.citationCount)
      // TODO: add corporate authors?
      val authorSpan = <span class="author">{ authors.mkString(", ") }</span>
      val titleSpan = <span class="title"><a href={ wokURLBase + wokID } target="_blank">{ title }</a></span>
      val sourceSpan = <span class="source">{ source }</span>
      val yearSpan = <span class="year">{ year }</span>
      val citationCountSpan = <span class="citation-count">Cited by { citationCount }</span>
      val parts = Seq(authorSpan, titleSpan, sourceSpan, yearSpan, citationCountSpan)
      <li>{ parts.map(_ ++ Text(". ")).flatten }</li>
    }

    // set the results element
    val foundHits = <div>Found { topDocs.totalHits } results</div>
    val articles = <ol>{ articleItems }</ol>
    val resultsDiv = <div id="results">{ foundHits ++ articles }</div>
    resultsDiv
  }
}
