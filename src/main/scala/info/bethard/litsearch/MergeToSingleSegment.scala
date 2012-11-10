package info.bethard.litsearch

import java.io.File
import scala.collection.JavaConverters._
import com.lexicalscope.jewel.cli.CliFactory
import com.lexicalscope.jewel.cli.{ Option => CliOption }
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.DirectoryReader

object MergeToSingleSegment {

  trait Options {
    @CliOption(longName = Array("indexes"))
    def getIndexFiles: java.util.List[File]
  }

  def main(args: Array[String]): Unit = {
    val options = CliFactory.parseArguments(classOf[Options], args: _*)
    val indexFiles = options.getIndexFiles.asScala
    val indexDirectories = indexFiles.map(FSDirectory.open)
    for (indexFile <- indexFiles) yield {
      val config = new IndexWriterConfig(IndexConfig.luceneVersion, IndexConfig.analyzer)
      config.setOpenMode(IndexWriterConfig.OpenMode.APPEND)
      val writer = new IndexWriter(FSDirectory.open(indexFile), config)
      writer.forceMerge(1)
      writer.close
    }
  }
}