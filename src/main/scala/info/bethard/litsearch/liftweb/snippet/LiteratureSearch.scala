package info.bethard.litsearch.liftweb.snippet

import java.io.File

import scala.Array.canBuildFrom
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
import net.liftweb.http.js.JsCmd
import net.liftweb.http.js.JsCmds
import net.liftweb.util.Helpers.strToCssBindPromoter
import net.liftweb.util.Helpers.tryo
import net.liftweb.util.Props

object LiteratureSearch {

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
    // variables that will be loaded from the HTML form inputs
    var query = ""
    var nHits = 10
    var textWeight = "0.47"
    var citationCountWeight = "1.0"
    var ageWeight = "-0.12"
    var topDocs: TopDocs = null

    // verify input values, and then return JavaScript that will post the results 
    def process(): JsCmd = {
      val stringWeightTuples = Seq(
        (textWeight, "text"),
        (citationCountWeight, "citation-count"),
        (ageWeight, "age"))

      // issue errors for any weights that aren't numbers
      for ((weight, name) <- stringWeightTuples; if tryo(weight.toFloat).isEmpty) {
        S.error(name + "-error", "The " + name + " weight must be a number")
      }

      // use the weights to get the results 
      val jsonBox = for {
        textWeight <- tryo(textWeight.toFloat)
        citationCountWeight <- tryo(citationCountWeight.toFloat)
        ageWeight <- tryo(ageWeight.toFloat)
        if !query.isEmpty
      } yield {

        // create the index with the given weights
        val index = new CombinedIndex(
          this.textIndex -> textWeight,
          this.citationCountIndex -> citationCountWeight,
          this.ageIndex -> ageWeight)

        // open the index for searching
        val articlesReader = open("index.articles")
        val citationCountReader = open("index.citation_count")

        // search for the given query
        topDocs = this.searcher.search(index.createQuery(query), nHits)

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
        JsCmds.SetHtml("results", foundHits ++ articles) & JsCmds.JsShowId("more")
      }
      jsonBox.openOr(JsCmds.Noop)
    }

    // set default input element values and register actions for storing modified values
    "name=query" #> SHtml.text(query, v => { query = v; nHits = 10 }) &
      "name=weight-text" #> SHtml.text(textWeight, v => { textWeight = v; nHits = 10 }) &
      "name=weight-citation-count" #> SHtml.text(citationCountWeight, v => { citationCountWeight = v; nHits = 10 }) &
      "name=weight-age" #> SHtml.text(ageWeight, v => { ageWeight = v; nHits = 10 }) &
      // register an action for increasing the number of hits
      "#more" #> SHtml.ajaxButton("More results", () => { nHits += 10; process }) &
      // verify the values that have been set and process the request
      "name=verify" #> SHtml.hidden(process)
  }
}
