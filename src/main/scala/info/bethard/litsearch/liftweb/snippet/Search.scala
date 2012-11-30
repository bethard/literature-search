package info.bethard.litsearch.liftweb.snippet

import java.io.File

import scala.xml.Elem
import scala.xml.Text

import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.index.ParallelCompositeReader
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.store.FSDirectory

import info.bethard.litsearch.IndexConfig
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

  // the base URL for linking articles to the Web of Knowledge
  val wokURLBase = "http://apps.webofknowledge.com/InboundService.do?product=WOS&action=retrieve&mode=FullRecord&UT="

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