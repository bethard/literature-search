package info.bethard.litsearch
import org.apache.lucene.analysis.PerFieldAnalyzerWrapper
import org.apache.lucene.analysis.standard.StandardAnalyzer
import scala.collection.JavaConverters._
import org.apache.lucene.util.Version.{ LUCENE_36 => luceneVersion }
import org.apache.lucene.analysis.KeywordAnalyzer
import org.apache.lucene.analysis.WhitespaceAnalyzer
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.store.Directory
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.queryParser.QueryParser

object IndexConfig {
  object FieldNames {
    val abstractText = "AbstractText"
    val titleText = "Title"
    val articleID = "ID"
    val articleIDWhenCited = "IDWhenCited"
    val citedArticleIDs = "CitedArticleIDs"
    val year = "Year"
    val citationCount = "CitationCount"
    val age = "Age"
  }

  val analyzer = new PerFieldAnalyzerWrapper(
    new StandardAnalyzer(luceneVersion),
    Map[String, Analyzer](
      FieldNames.abstractText -> new StandardAnalyzer(luceneVersion),
      FieldNames.citedArticleIDs -> new WhitespaceAnalyzer(luceneVersion),
      FieldNames.citationCount -> new KeywordAnalyzer).asJava)

  def newIndexWriter(directory: Directory): IndexWriter = {
    val config = new IndexWriterConfig(luceneVersion, this.analyzer)
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
    new IndexWriter(directory, config)
  }

  def newQueryParser(defaultField: String): QueryParser = {
    new QueryParser(luceneVersion, defaultField, this.analyzer)
  }
}