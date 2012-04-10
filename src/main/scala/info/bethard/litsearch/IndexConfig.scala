package info.bethard.litsearch
import org.apache.lucene.analysis.PerFieldAnalyzerWrapper
import org.apache.lucene.analysis.standard.StandardAnalyzer
import scala.collection.JavaConverters._
import org.apache.lucene.util.Version
import org.apache.lucene.analysis.KeywordAnalyzer
import org.apache.lucene.analysis.WhitespaceAnalyzer
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.store.Directory
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.queryParser.QueryParser

object IndexConfig {
  object FieldNames {
    val articleID = "ArticleID"
    val citedArticleIDs = "CitedArticleIDs"
    val year = "Year"
    val citationCount = "CitationCount"
    val age = "Age"
  }
  val analyzer = new PerFieldAnalyzerWrapper(
    new StandardAnalyzer(Version.LUCENE_35),
    Map[String, Analyzer](
      FieldNames.citedArticleIDs -> new WhitespaceAnalyzer(Version.LUCENE_35),
      FieldNames.citationCount -> new KeywordAnalyzer).asJava)

  def newIndexWriter(directory: Directory): IndexWriter = {
    val config = new IndexWriterConfig(Version.LUCENE_35, this.analyzer)
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
    new IndexWriter(directory, config)
  }

  def newQueryParser(defaultField: String): QueryParser = {
    new QueryParser(Version.LUCENE_35, defaultField, this.analyzer)
  }
}