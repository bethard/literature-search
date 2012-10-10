package info.bethard.litsearch.webofknowledge

import org.apache.lucene.store.Directory
import org.apache.lucene.index.IndexWriter
import info.bethard.litsearch.IndexConfig
import java.io.Closeable
import org.apache.lucene.document.Document
import org.apache.lucene.document.Field
import org.apache.lucene.document.TextField
import org.apache.lucene.document.StringField
import org.apache.lucene.document.IntField
import org.apache.lucene.store.FSDirectory
import scalax.file.Path
import org.apache.lucene.index.IndexWriterConfig

object WebOfKnowledgeIndexer {

  def main(args: Array[String]) = {
    if (args.length < 2) {
      val message = "usage: java %s index-dir wok-dir [wok-dir ...]"
      throw new IllegalArgumentException(message.format(this.getClass.getName))
    }
    val indexPath = Path.fromString(args.head)
    if (indexPath.exists) {
      throw new IllegalArgumentException("directory already exists: " + indexPath.path)
    }
    val wokPaths = args.toSeq.tail.map(Path.fromString)
    val indexer = new WebOfKnowledgeIndexer(indexPath)
    indexer.parse(wokPaths)
  }
}

class WebOfKnowledgeIndexer(indexPath: Path) extends WebOfKnowledgeParser with Closeable {
  import info.bethard.litsearch.webofknowledge.WebOfKnowledgeParser._

  var indexDirectory: Directory = null
  var indexWriter: IndexWriter = null

  object Fields {
    import info.bethard.litsearch.IndexConfig.FieldNames
    val abstractText = new TextField(FieldNames.abstractText, "", Field.Store.YES)
    val titleText = new TextField(FieldNames.titleText, "", Field.Store.YES)
    val sourceTitleText = new TextField(FieldNames.sourceTitleText, "", Field.Store.YES)
    val authors = new TextField(FieldNames.authors, "", Field.Store.YES)
    val articleID = new StringField(FieldNames.articleID, "", Field.Store.YES)
    val articleIDWhenCited = new StringField(FieldNames.articleIDWhenCited, "", Field.Store.YES)
    val citedArticleIDs = new TextField(FieldNames.citedArticleIDs, "", Field.Store.YES)
    val year = new IntField(FieldNames.year, -1, Field.Store.YES)
  }

  override def close = this.indexWriter.close

  override def beginFile(file: File) = {
    println(file.name)
    val config = new IndexWriterConfig(IndexConfig.luceneVersion, IndexConfig.analyzer)
    this.indexDirectory = FSDirectory.open(this.indexPath.fileOption.get)
    this.indexWriter = IndexConfig.newIndexWriter(this.indexDirectory)
  }

  override def endFile(file: File) = {
    this.indexWriter.close
    this.indexDirectory.close
  }

  override def endItem(item: Item) = {
    val document = new Document

    // set article ID
    Fields.articleID.setStringValue(item.identifier.trim)
    document.add(Fields.articleID)

    // set article ID that is used for citation (if present)
    for (id <- item.identifierForReferences) {
      Fields.articleIDWhenCited.setStringValue(id.trim)
      document.add(Fields.articleIDWhenCited)
    }

    // set title
    Fields.titleText.setStringValue(item.title.toString)
    document.add(Fields.titleText)

    // set issue title
    Fields.sourceTitleText.setStringValue(item.issue.title.toString)
    document.add(Fields.sourceTitleText)

    // set year
    Fields.year.setIntValue(item.issue.publicationYear.get)
    document.add(Fields.year)

    // set authors, normalizing and putting whitespace between (if present)
    val normalize: (String => String) = {
      _.trim.toLowerCase.replaceAll("[\\s\\p{P}&&[^,]]", "").replaceAll(",", "_")
    }
    val authors = item.authors.map(normalize).mkString(" ")
    if (!authors.isEmpty) {
      Fields.authors.setStringValue(authors)
      document.add(Fields.authors)
    }

    // set abstract from paragraphs (if present)
    val abstractText = item.abstractText.mkString("\n")
    if (!abstractText.isEmpty) {
      Fields.abstractText.setStringValue(abstractText)
      document.add(Fields.abstractText)
    }

    // set IDs of cited articles, putting whitespace in between (if present)
    val ids = item.citedReferences.flatMap(_.identifierForReferences.iterator)
    if (!ids.isEmpty) {
      Fields.citedArticleIDs.setStringValue(ids.mkString(" "))
      document.add(Fields.citedArticleIDs)
    }
  }
}