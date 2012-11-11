package info.bethard.litsearch.webofknowledge

import java.io.File

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.lucene.document.Field
import org.apache.lucene.document.IntField
import org.apache.lucene.index.AtomicReaderContext
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.index.ParallelCompositeReader
import org.apache.lucene.search.Collector
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.Scorer
import org.apache.lucene.store.FSDirectory

import com.lexicalscope.jewel.cli.CliFactory
import com.lexicalscope.jewel.cli.{ Option => CliOption }

import info.bethard.litsearch.IndexConfig
import info.bethard.litsearch.IndexConfig.FieldNames
import info.bethard.litsearch.TitleAbstractTextIndex

object SubIndexWriter {

  trait Options {
    @CliOption(longName = Array("source-indexes"))
    def getIndexFiles: java.util.List[File]

    @CliOption(longName = Array("output-dir"))
    def getOutputDirectory: File

    @CliOption(longName = Array("query"), defaultValue = Array("preterm premature prematurity"))
    def getQuery: String
  }

  def main(args: Array[String]): Unit = {
    val options = CliFactory.parseArguments(classOf[Options], args: _*)

    // the documents selected by the query
    val docIDs = mutable.Buffer.empty[Int]

    // open the reader
    val indexFiles = options.getIndexFiles.asScala
    val subReaders = for (f <- indexFiles) yield DirectoryReader.open(FSDirectory.open(f))
    val reader = new ParallelCompositeReader(subReaders: _*)

    // search for documents matching the query
    try {
      val textIndex = new TitleAbstractTextIndex
      val searcher = new IndexSearcher(reader)
      searcher.search(textIndex.createQuery(options.getQuery), new Collector {
        override def setScorer(scorer: Scorer) = {}
        override def collect(docID: Int) {
          docIDs += docID
        }
        override def setNextReader(context: AtomicReaderContext) = {}
        override def acceptsDocsOutOfOrder = true
      })
    } finally {
      reader.close()
    }

    // write the selected documents from each index
    printf("Writing %d documents\n", docIDs.size)
    for (inputFile <- indexFiles) {
      val outputFile = new File(options.getOutputDirectory, inputFile.getName)
      System.err.println("Writing index " + outputFile)
      val reader = DirectoryReader.open(FSDirectory.open(inputFile))
      try {
        val writer = IndexConfig.newIndexWriter(FSDirectory.open(outputFile))
        try {
          // reporting increment - report progress 10 times
          val incr = math.pow(10, math.floor(math.log10(docIDs.size / 10)))
          var i = -1
          var time = System.nanoTime
          
          // write each selected document from the reader to the writer
          for (docID <- docIDs) {
            // FIXME: this workaround is necessary or the the year tokens don't get indexed
            val fields = for (field <- reader.document(docID).asScala) yield {
              if (field.name() == FieldNames.year) {
                new IntField(FieldNames.year, field.numericValue.intValue, Field.Store.YES)
              } else {
                field
              }
            }
            writer.addDocument(fields.asJava)

            // report progress
            i += 1
            if (i % incr == 0) {
              val elapsedSeconds = (System.nanoTime - time) / 1e9
              val message = "added: %d/%d (%.1f seconds)"
              System.err.println(message.format(i, docIDs.size, elapsedSeconds))
              time = System.nanoTime
            }
          }
        } finally {
          writer.close
        }
      } finally {
        reader.close
      }
    }
  }

}