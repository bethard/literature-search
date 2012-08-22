package info.bethard.litsearch

import java.io.File
import org.apache.lucene.document.Document
import org.apache.lucene.document.Field
import org.apache.lucene.document.TextField
import org.apache.lucene.index.IndexReader
import org.apache.lucene.queries.function.FunctionQuery
import org.apache.lucene.queries.function.valuesource.IntFieldSource
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.Query
import org.apache.lucene.store.Directory
import com.lexicalscope.jewel.cli.{ Option => CliOption }
import com.lexicalscope.jewel.cli.CliFactory
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.index.DirectoryReader

class CitationCountIndex extends Index {

  override def createQuery(queryText: String): Query = {
    new FunctionQuery(new IntFieldSource(IndexConfig.FieldNames.citationCount))
  }
}

object CitationCountIndex {

  def buildFrom(articleIndexReader: IndexReader, citationCountIndexDirectory: Directory): Unit = {
    val citationCountIndexWriter = IndexConfig.newIndexWriter(citationCountIndexDirectory)
    try {
      val maxDoc = articleIndexReader.maxDoc
      val articleIndexSearcher = new IndexSearcher(articleIndexReader)
      val getIDsQuery = IndexConfig.parseTermsQuery(IndexConfig.FieldNames.citedArticleIDs, _: String)
      for (i <- 0 until maxDoc) {
        val document = articleIndexSearcher.doc(i)
        val articleIDWhenCited = document.get(IndexConfig.FieldNames.articleIDWhenCited)
        val citationCountOption = Option(articleIDWhenCited).map(id =>
          articleIndexSearcher.search(getIDsQuery(id), maxDoc).totalHits)

        val doc = new Document
        doc.add(new TextField(
          IndexConfig.FieldNames.citationCount,
          citationCountOption.getOrElse(0).toString,
          Field.Store.YES))
        citationCountIndexWriter.addDocument(doc)
      }
    } finally {
      citationCountIndexWriter.close
    }
  }

  trait Options {
    @CliOption(longName = Array("article-index"))
    def getArticleIndex: File

    @CliOption(longName = Array("citation-count-index"), defaultToNull = true)
    def getCitationCountIndex: File
  }

  def main(args: Array[String]): Unit = {
    // parse arguments
    val options = CliFactory.parseArguments(classOf[Options], args: _*)
    val citationCountIndex = Option(options.getCitationCountIndex).getOrElse(
      new File(options.getArticleIndex.getPath() + "-citation-count"))

    // build citation count index from article index
    val citationCountIndexDirectory = FSDirectory.open(citationCountIndex)
    try {
      val articleIndexDirectory = FSDirectory.open(options.getArticleIndex)
      try {
        val articleIndexReader = DirectoryReader.open(articleIndexDirectory)
        try {
          this.buildFrom(articleIndexReader, citationCountIndexDirectory)
        } finally {
          articleIndexReader.close
        }
      } finally {
        articleIndexDirectory.close
      }
    } finally {
      citationCountIndexDirectory.close
    }
  }

}