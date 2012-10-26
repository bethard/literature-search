package info.bethard.litsearch.webofknowledge

import org.apache.lucene.store.Directory
import org.apache.lucene.index.IndexWriter
import info.bethard.litsearch.IndexConfig
import info.bethard.litsearch.IndexConfig.FieldNames
import java.io.Closeable
import org.apache.lucene.document.Document
import org.apache.lucene.document.Field
import org.apache.lucene.document.TextField
import org.apache.lucene.document.StringField
import org.apache.lucene.document.IntField
import org.apache.lucene.store.FSDirectory
import scalax.file.Path
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.index.Term
import java.util.logging.Logger
import org.apache.lucene.search.IndexSearcher

object WebOfKnowledgeIndexer {

  def main(args: Array[String]) = {
    if (args.length < 2) {
      val message = "usage: java %s index-dir wok-dir [wok-dir ...]"
      throw new IllegalArgumentException(message.format(this.getClass.getName))
    }
    val indexPath = Path.fromString(args.head)
    val wokPaths = args.toSeq.tail.map(Path.fromString)
    val indexer = new WebOfKnowledgeIndexer(indexPath)
    try {
      indexer.parse(wokPaths)
    } finally {
      indexer.close
    }
  }
}

class WebOfKnowledgeIndexer(indexPath: Path) extends WebOfKnowledgeParser with Closeable {
  import info.bethard.litsearch.webofknowledge.WebOfKnowledgeParser._
  
  val logger = Logger.getLogger(this.getClass.getName)

  val config = new IndexWriterConfig(IndexConfig.luceneVersion, IndexConfig.analyzer)
  config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
  config.setRAMBufferSizeMB(1000)
  val indexWriter = new IndexWriter(FSDirectory.open(indexPath.fileOption.get), config)
  override def close = this.indexWriter.close

  object Fields {
    val abstractText = new TextField(FieldNames.abstractText, "", Field.Store.YES)
    val titleText = new TextField(FieldNames.titleText, "", Field.Store.YES)
    val sourceID = new StringField(FieldNames.sourceID, "", Field.Store.YES)
    val sourceTitleText = new TextField(FieldNames.sourceTitleText, "", Field.Store.YES)
    val authors = new TextField(FieldNames.authors, "", Field.Store.YES)
    val articleID = new StringField(FieldNames.articleID, "", Field.Store.YES)
    val articleIDWhenCited = new StringField(FieldNames.articleIDWhenCited, "", Field.Store.YES)
    val citedArticleIDs = new TextField(FieldNames.citedArticleIDs, "", Field.Store.YES)
    val year = new IntField(FieldNames.year, -1, Field.Store.YES)
  }

  override def beginFile(file: File) = {
    this.logger.info(file.name)
  }
  
  override def endIssueInfo(issue: Issue) = {
    import info.bethard.litsearch.IndexConfig.FieldNames
    import IssueTransaction._
    issue.transaction.map(_ match {
      case NoAction | Insert => // issue will be inserted normally
      case Delete | Replace => {
        val docsBefore = this.indexWriter.maxDoc();
        this.indexWriter.deleteDocuments(new Term(FieldNames.sourceID, issue.identifier.trim))
        val docsDeleted = docsBefore - this.indexWriter.maxDoc()
        this.logger.info("Deleted all %d item(s) from issue %s".format(docsDeleted, issue.identifier))
      }
      case Change => {
        // TODO: find all instances of issue ID, and update issue metadata (e.g. SourceTitle)
        // This is more complicated because you need to create a searcher, etc.
        this.logger.info("Updating issue metadata is not yet implemented: " + issue.identifier)
      }
    })
  }

  override def endItem(item: Item) = {
    // delete the item first if necessary
    import ItemTransaction._
    item.transaction.map(_ match {
      case Insert => // item will be inserted normally (below)
      case Replace | Change => {
        val docsBefore = this.indexWriter.maxDoc()
        this.indexWriter.deleteDocuments(new Term(FieldNames.articleID, item.identifier.trim))
        val docsDeleted = docsBefore - this.indexWriter.maxDoc()
        this.logger.info("Deleted %d item(s) for item %s".format(docsDeleted, item.identifier))
      } 
    })
    
    
    val document = new Document

    // set article ID
    Fields.articleID.setStringValue(item.identifier.trim)
    document.add(Fields.articleID)
    
    // set source ID
    Fields.sourceID.setStringValue(item.issue.identifier.trim)
    document.add(Fields.sourceID)

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

    // add the document to the index
    this.indexWriter.addDocument(document)
  }
  
  override def endIssue(issue: Issue) = {
    this.indexWriter.commit
  }
}