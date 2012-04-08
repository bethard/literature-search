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

class CitationCountIndex(directory: Directory) {

  def buildFrom(inputReader: IndexReader): Unit = {
    val inputSearcher = new IndexSearcher(inputReader)
    val writer = IndexConfig.newIndexWriter(directory)
    val queryParser = IndexConfig.newQueryParser(IndexConfig.FieldNames.citedArticleIDs)
    for (i <- 0 until inputReader.maxDoc) {
      val query = queryParser.parse(inputSearcher.doc(i).get(IndexConfig.FieldNames.articleID))
      val citationCount = inputSearcher.search(query, inputSearcher.maxDoc).totalHits
      val doc = new Document
      doc.add(new Field(
        IndexConfig.FieldNames.citationCount,
        citationCount.toString,
        Field.Store.YES,
        Field.Index.NOT_ANALYZED))
      writer.addDocument(doc)
    }
    writer.close()
  }

  def reader: IndexReader = IndexReader.open(directory)

  def query: Query = new ValueSourceQuery(new IntFieldSource(IndexConfig.FieldNames.citationCount))
}