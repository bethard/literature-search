package info.bethard.litsearch.liftweb.snippet

import java.io.File

import scala.Array.canBuildFrom
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

  private object query extends SessionVar("") {
    override def set(value: String) = {
      if (value != this.is) {
        nHits.set(10)
      }
      super.set(value)
    }
  }
  private object results extends SessionVar[Box[TopDocs]](Empty)

  private object nHits extends SessionVar(10)
  private object textWeight extends SessionVar("0.47")
  private object citationCountWeight extends SessionVar("1.0")
  private object ageWeight extends SessionVar("-0.12")

  val wokURLBase = "http://apps.webofknowledge.com/InboundService.do?product=WOS&action=retrieve&mode=FullRecord&UT="

  val textIndex = new TitleAbstractTextIndex
  val citationCountIndex = new CitationCountIndex
  val ageIndex = new AgeIndex(2012)

  val articlesReader = open("index.articles")
  val citationCountReader = open("index.citation_count")
  val reader = new ParallelCompositeReader(articlesReader, citationCountReader)
  val searcher = new IndexSearcher(reader)

  private def open(name: String): DirectoryReader = {
    val path = Props.get(name).failMsg("\"%s\" property missing".format(name)).open_!
    DirectoryReader.open(FSDirectory.open(new File(path)))
  }

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
          this.citationCountIndex -> citationCountWeight,
          this.ageIndex -> ageWeight)

        // open the index for searching
        val articlesReader = open("index.articles")
        val citationCountReader = open("index.citation_count")

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
      val authors = doc.get(IndexConfig.FieldNames.authors).split(" ").map { authorString =>
        authorString.split("_") match {
          case Array() => ""
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
      // TODO: add corporate authors?
      val authorSpan = <span class="author">{ authors.mkString(", ") }</span>
      val titleSpan = <span class="title"><a href={ wokURLBase + wokID } target="_blank">{ title }</a></span>
      val sourceSpan = <span class="source">{ source }</span>
      val yearSpan = <span class="year">{ year }</span>
      val parts = Seq(authorSpan, titleSpan, sourceSpan, yearSpan)
      <li>{ parts.map(_ ++ Text(". ")).flatten }</li>
    }

    // set the results element
    val foundHits = <div>Found { topDocs.totalHits } results</div>
    val articles = <ol>{ articleItems }</ol>
    val resultsDiv = <div id="results">{ foundHits ++ articles }</div>
    resultsDiv
  }
}
