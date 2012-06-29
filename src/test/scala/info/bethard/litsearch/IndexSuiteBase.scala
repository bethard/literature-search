package info.bethard.litsearch
import java.io.File
import org.scalatest.FunSuite
import org.apache.lucene.index.IndexReader
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.document.Document
import org.apache.lucene.document.Field
import org.apache.lucene.store.Directory

abstract class IndexSuiteBase extends FunSuite {

  def temporaryDirectory: Traversable[File] = new Traversable[File] {
    override def foreach[U](f: File => U): Unit = {
      // create the temporary directory
      val dir = File.createTempFile("temp", "dir")
      dir.delete()
      dir.mkdirs()

      // produce the directory as the next item in the Traversable
      f(dir)

      // delete the temporary directory
      this.delete(dir)
    }

    def delete(file: File): Unit = {
      // delete child files recursively
      Option(file.listFiles).flatten.map(this.delete)
      // delete this file (and assert that it was actually deleted)
      assert(file.delete() === true)
    }
  }

  def temporaryFSDirectory: Traversable[Directory] = {
    this.temporaryDirectory.map(FSDirectory.open)
  }

  def temporaryIndexReader(docs: Seq[(String, String)]*): Traversable[IndexReader] = new Traversable[IndexReader] {
    override def foreach[U](f: IndexReader => U): Unit = {
      for (indexDir <- temporaryDirectory) {

        // create the index from the given documents
        val fsDir = FSDirectory.open(indexDir)
        writeDocuments(fsDir, docs: _*)

        // create the reader
        val reader = IndexReader.open(fsDir)

        // produce the reader as the next item in the Traversable
        f(reader)

        // close the reader
        reader.close
      }
    }
  }

  def writeDocuments(indexDir: Directory, docs: Seq[(String, String)]*) = {
    val writer = IndexConfig.newIndexWriter(indexDir)
    for (doc <- docs) {
      val document = new Document
      for ((key, value) <- doc) {
        document.add(new Field(key, value, Field.Store.YES, Field.Index.ANALYZED))
      }
      writer.addDocument(document)
    }
    writer.close
  }
}
