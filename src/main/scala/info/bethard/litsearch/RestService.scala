package info.bethard.litsearch

import net.liftweb.common.Box
import net.liftweb.http.{ S, Req, GetRequest, PostRequest }
import net.liftweb.http.rest.RestHelper
import net.liftweb.util.Helpers.tryo
import net.liftweb.util.Props
import org.slf4j.LoggerFactory
import org.apache.lucene.store.Directory
import org.apache.lucene.store.FSDirectory
import java.io.File
import org.apache.lucene.search.IndexSearcher
import scala.collection.JavaConverters._
import scala.xml.{ Elem, Node, Null, Text, TopScope }
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.index.ParallelCompositeReader

object RestService extends RestHelper {
  private val logger = LoggerFactory.getLogger(this.getClass())

  // load configuration from properties
  private def open(name: String): DirectoryReader = {
    val path = Props.get(name).failMsg("\"%s\" property missing".format(name)).open_!
    DirectoryReader.open(FSDirectory.open(new File(path)))
  }

  val titleIndex = new TitleTextIndex
  val abstractIndex = new AbstractTextIndex
  val citationCountIndex = new CitationCountIndex
  val ageIndex = new AgeIndex(2012)

  serve {
    case Req("api" :: "literature-search" :: Nil, "xml", GetRequest | PostRequest) => {

      // parse query parameters
      for {
        queryPhrase <- this.getParam("query")
        titleWeight <- this.getFloatParam("weight_title")
        abstractWeight <- this.getFloatParam("weight_abstract")
        citationCountWeight <- this.getFloatParam("weight_citation_count")
        ageWeight <- this.getFloatParam("weight_age")
      } yield {

        // create the index with the requested weights
        val index = new CombinedIndex(
          this.titleIndex -> titleWeight,
          this.abstractIndex -> abstractWeight,
          this.citationCountIndex -> citationCountWeight,
          this.ageIndex -> ageWeight)

        // open the index for searching
        val articlesReader = open("index.articles")
        val citationCountReader = open("index.citation_count")
        val reader = new ParallelCompositeReader(articlesReader, citationCountReader)
        try {
          // search for the given query
          val searcher = new IndexSearcher(reader)
          val topDocs = searcher.search(index.createQuery(queryPhrase), 10)

          // convert document results to XML
          val articleElems = for (scoreDoc <- topDocs.scoreDocs) yield {
            val doc = reader.document(scoreDoc.doc)
            val fieldNames = doc.getFields.asScala.map(_.name).distinct
            val fieldElems = for (fieldName <- fieldNames) yield {
              Elem(null, fieldName, Null, TopScope, Text(doc.get(fieldName)))
            }
            Elem(null, "article", Null, TopScope, addNewlines(fieldElems): _*)
          }
          Elem(null, "articles", Null, TopScope, addNewlines(articleElems): _*)

        } finally {
          // close index
          reader.close()
        }
      }
    }
  }

  private def addNewlines(elems: Seq[Elem]): Seq[Node] = {
    val nl = Text("\n")
    Seq(nl) ++ elems.flatMap(e => Seq(e, nl))
  }

  private def getParam(name: String): Box[String] = {
    S.param(name).filter(!_.isEmpty) ?~ "'%s' parameter missing".format(name) ~> 400
  }

  private def getFloatParam(name: String): Box[Float] = {
    this.getParam(name).flatMap(this.toFloatBox)
  }

  private def toFloatBox(string: String) = tryo(classOf[NumberFormatException])(string.toFloat)
}