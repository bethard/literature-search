package info.bethard.litsearch.liftweb.snippet

import java.io.File

import scala.xml.Elem
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
import info.bethard.litsearch.TitleAbstractTextIndex
import net.liftweb.http.S
import net.liftweb.http.SHtml
import net.liftweb.http.SessionVar
import net.liftweb.util.Helpers.strToCssBindPromoter
import net.liftweb.util.Helpers.tryo
import net.liftweb.util.Props

object Search {
  // a combined reader to all indexes specified in the properties
  val reader = {
    val pathsString = Props.get("indexes").failMsg("\"indexes\" property missing").open_!
    val indexReaders = for (indexPath <- pathsString.split(",")) yield {
      DirectoryReader.open(FSDirectory.open(new File(indexPath)))
    }
    new ParallelCompositeReader(indexReaders: _*)
  }

  // a searcher for the combined reader
  val searcher = new IndexSearcher(reader)

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

  class SearchResettingSessionVar[T](value: T) extends SessionVar(value) {
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
  object textWeight extends SearchResettingSessionVar(learnedTextWeightString)
  object citationCountWeight extends SearchResettingSessionVar(learnedCitationCountWeightString)
  object ageWeight extends SearchResettingSessionVar(learnedAgeWeightString)

  def render = {
    "#results" #> this.renderResults.getOrElse(NodeSeq.Empty) &
      "name=query" #> SHtml.textElem(query) &
      "name=weight-text" #> SHtml.textElem(textWeight) &
      "name=weight-citation-count" #> SHtml.textElem(citationCountWeight) &
      "name=weight-age" #> SHtml.textElem(ageWeight) &
      "#more" #> this.increaseNHits &
      "#submit" #> SHtml.onSubmitUnit(() => this.search)
  }

  def increaseNHits: NodeSeq => NodeSeq = {
    if (results.is.isEmpty) {
      "* [style]" #> "display:none" 
    } else {
      SHtml.onSubmitUnit{() =>
        nHits.update(_ + 10)
        this.search
      }
    }
  }

  def search: Unit = {
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
      if !query.is.isEmpty
    } {

      // create the index with the given weights
      val index = new CombinedIndex(
        this.textIndex -> textWeight,
        this.citationCountIndex -> citationCountWeight / this.citationCountScalingFactor,
        this.ageIndex -> ageWeight / this.ageScalingFactor)

      // search for the given query
      val topDocs = Search.searcher.search(index.createQuery(query.is), nHits.is)
      results.set(Some(topDocs))
    }
  }

  def renderResults: Option[Elem] = {
    for (topDocs <- results.is) yield {
      val articleElemIter = topDocs.scoreDocs.iterator.map(_.doc).map(Search.renderArticle)
      val foundHits = <div>Found { topDocs.totalHits } results</div>
      val articles = <ol>{ articleElemIter.map(elem => <li>{ elem }</li>) }</ol>
      val resultsDiv = <div>{ foundHits ++ articles }</div>
      resultsDiv
    }
  }

  // the base URL for linking articles to the Web of Knowledge
  private val wokURLBase = "http://apps.webofknowledge.com/InboundService.do?product=WOS&action=retrieve&mode=FullRecord&UT="

  // formats an article as HTML
  def renderArticle(docID: Int): Elem = {
    val doc = Search.reader.document(docID)
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
    <span>{ parts.map(_ ++ Text(". ")).flatten }</span>
  }
}