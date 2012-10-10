package info.bethard.litsearch

import java.io.StringReader

import scala.collection.JavaConverters._

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.core.KeywordAnalyzer
import org.apache.lucene.analysis.core.WhitespaceAnalyzer
import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.index.Term
import org.apache.lucene.search.BooleanClause
import org.apache.lucene.search.BooleanQuery
import org.apache.lucene.search.Query
import org.apache.lucene.search.TermQuery
import org.apache.lucene.store.Directory
import org.apache.lucene.util.Version.LUCENE_40

object IndexConfig {
  val luceneVersion = LUCENE_40

  object FieldNames {
    val abstractText = "AbstractText"
    val titleText = "Title"
    val sourceTitleText = "SourceTitle"
    val authors = "Authors"
    val articleID = "ID"
    val articleIDWhenCited = "IDWhenCited"
    val citedArticleIDs = "CitedArticleIDs"
    val year = "Year"
    val citationCount = "CitationCount"
  }

  val analyzer = new PerFieldAnalyzerWrapper(
    new EnglishAnalyzer(luceneVersion),
    Map[String, Analyzer](
      FieldNames.authors -> new WhitespaceAnalyzer(luceneVersion),
      FieldNames.articleID -> new KeywordAnalyzer,
      FieldNames.articleIDWhenCited -> new KeywordAnalyzer,
      FieldNames.citedArticleIDs -> new WhitespaceAnalyzer(luceneVersion),
      FieldNames.citationCount -> new KeywordAnalyzer).asJava)

  def newIndexWriter(directory: Directory): IndexWriter = {
    val config = new IndexWriterConfig(luceneVersion, this.analyzer)
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
    new IndexWriter(directory, config)
  }

  def parseTermsQuery(fieldName: String, queryText: String): Query = {
    val tokenStream = this.analyzer.tokenStream(fieldName, new StringReader(queryText))
    val termAttribute = tokenStream.getAttribute(classOf[CharTermAttribute]);
    val incrementIter = Iterator.continually(tokenStream.incrementToken)
    val query = new BooleanQuery
    while (tokenStream.incrementToken) {
      val term = new Term(fieldName, termAttribute.toString)
      query.add(new TermQuery(term), BooleanClause.Occur.SHOULD)
    }
    query
  }
}
