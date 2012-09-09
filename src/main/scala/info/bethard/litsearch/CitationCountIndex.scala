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
import org.apache.lucene.search.Collector
import scala.collection.mutable.Buffer
import org.apache.lucene.search.Scorer
import org.apache.lucene.index.AtomicReaderContext
import org.apache.lucene.search.TotalHitCountCollector

class CitationCountIndex extends Index {

  override def createQuery(queryText: String): Query = {
    new FunctionQuery(new IntFieldSource(IndexConfig.FieldNames.citationCount))
  }
}

object CitationCountIndex {

  def buildFrom(articleIndexReader: IndexReader, citationCountIndexDirectory: Directory): Unit = {
    // total number of documents in the index
    val maxDoc = articleIndexReader.maxDoc
    // reporting increment - report progress 1000 times
    val incr = math.pow(10, math.floor(math.log10(maxDoc / 1000)))
    // function to parse an article ID into a query for citing articles
    val getIDsQuery = IndexConfig.parseTermsQuery(IndexConfig.FieldNames.citedArticleIDs, _: String)

    // create index of citation count for each article
    val citationCountIndexWriter = IndexConfig.newIndexWriter(citationCountIndexDirectory)
    try {
      val articleIndexSearcher = new IndexSearcher(articleIndexReader)
      var time = System.nanoTime
      for (i <- 0 until maxDoc) {

        // report progress
        if (i % incr == 0) {
          val elapsedSeconds = (System.nanoTime - time) / 1e9
          System.err.println("indexed: %d/%d (%.1f seconds)".format(i, maxDoc, elapsedSeconds))
          time = System.nanoTime
        }

        // query for citing articles and count them
        val document = articleIndexSearcher.doc(i)
        val articleIDWhenCited = document.get(IndexConfig.FieldNames.articleIDWhenCited)
        val citationCountOption = Option(articleIDWhenCited).map(id => {
          val collector = new TotalHitCountCollector
          articleIndexSearcher.search(getIDsQuery(id), collector)
          collector.getTotalHits
        })

        // add a document field with the citation count
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
