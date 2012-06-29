package info.bethard.litsearch
import org.apache.lucene.index.IndexReader
import org.apache.lucene.store.Directory
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.analysis.KeywordAnalyzer
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.util.Version
import org.apache.lucene.document.Document
import org.apache.lucene.document.Field
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.queryParser.QueryParser
import org.apache.lucene.search.Query
import org.apache.lucene.search.function.ValueSourceQuery
import org.apache.lucene.search.function.IntFieldSource

class CitationCountIndex(directory: Directory) extends Index {

  def buildFrom(inputReader: IndexReader): Unit = {
    val inputSearcher = new IndexSearcher(inputReader)
    val writer = IndexConfig.newIndexWriter(directory)
    val queryParser = IndexConfig.newQueryParser(IndexConfig.FieldNames.citedArticleIDs)
    for (i <- 0 until inputReader.maxDoc) {
      val document = inputSearcher.doc(i)
      val articleIDWhenCited = document.get(IndexConfig.FieldNames.articleIDWhenCited)
      val citationCountOption = Option(articleIDWhenCited).map(id =>
        inputSearcher.search(queryParser.parse(id), inputSearcher.maxDoc).totalHits)

      val doc = new Document
      doc.add(new Field(
        IndexConfig.FieldNames.citationCount,
        citationCountOption.getOrElse(0).toString,
        Field.Store.YES,
        Field.Index.NOT_ANALYZED))
      writer.addDocument(doc)
    }
    writer.close()
  }

  override def openReader(): IndexReader = IndexReader.open(directory)

  override def createQuery(queryText: String): Query = {
    new ValueSourceQuery(new IntFieldSource(IndexConfig.FieldNames.citationCount))
  }
}