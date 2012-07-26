package info.bethard.litsearch

import org.apache.lucene.document.Document
import org.apache.lucene.document.Field
import org.apache.lucene.document.TextField
import org.apache.lucene.index.IndexReader
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.store.Directory
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.Query
import org.apache.lucene.util.Version
import org.apache.lucene.queries.function.FunctionQuery
import org.apache.lucene.queries.function.valuesource.IntFieldSource

class CitationCountIndex extends Index {

  def buildFrom(inputReader: IndexReader, outputDirectory: Directory): Unit = {
    val inputSearcher = new IndexSearcher(inputReader)
    val writer = IndexConfig.newIndexWriter(outputDirectory)
    val getIDsQuery = IndexConfig.parseTermsQuery(IndexConfig.FieldNames.citedArticleIDs, _: String)
    for (i <- 0 until inputReader.maxDoc) {
      val document = inputSearcher.doc(i)
      val articleIDWhenCited = document.get(IndexConfig.FieldNames.articleIDWhenCited)
      val citationCountOption = Option(articleIDWhenCited).map(id =>
        inputSearcher.search(getIDsQuery(id), inputReader.maxDoc).totalHits)

      val doc = new Document
      doc.add(new TextField(
        IndexConfig.FieldNames.citationCount,
        citationCountOption.getOrElse(0).toString,
        Field.Store.YES))
      writer.addDocument(doc)
    }
    writer.close()
  }

  override def createQuery(queryText: String): Query = {
    new FunctionQuery(new IntFieldSource(IndexConfig.FieldNames.citationCount))
  }
}