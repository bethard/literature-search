package info.bethard.litsearch.webofknowledge

import java.util.zip.GZIPInputStream
import resource.ExtractableManagedResource
import scalax.file.Path
import scalax.io.Input
import scalax.io.JavaConverters._
import scala.collection.mutable.Buffer

object IndexWebOfKnowledge {

  def main(args: Array[String]): Unit = {
    val paths = for (dir <- args.toList; path <- Path.fromString(dir).children()) yield path
    val getDate: Path => String = withLinesIterator(_) {
      _.filter(_.code == Code.File.CreationDate).map(_.content).next
    }
    for (path <- paths.sortBy(getDate)) {
      println(path.path)
      withLinesIterator(path) { lines =>
        val line = lines.next
        assert(line.code == Code.File.Name)
        this.parseFile(lines)
      }
    }
  }

  def parseFile(lines: Iterator[Line]) = {
    val file = new File
    for (line <- lines.takeWhile(_.code != Code.File.End)) line.code match {
      case Code.File.Name => file.name = Some(line.content)
      case Code.File.Copyright => file.copyright = Some(line.content)
      case Code.File.HeaderFileType => file.headerFileType = Some(line.content)
      case Code.File.CreationDate => file.creationDate = Some(line.content)
      case Code.File.Version => file.version = Some(line.content)
      case Code.File.ProductCode => file.productCodes += line.content
      case Code.File.CustomizationDate => file.customizationDate = Some(line.content)
      case Code.File.ProductionWeeks => file.productionWeeks = Some(line.content)
      case Code.File.Periodicity => file.periodicity = Some(line.content)
      case Code.File.IssueCount => file.issueCount = Some(line.content.toInt)
      case Code.File.ItemCount => file.itemCount = Some(line.content.toInt)
      case Code.File.LineCount => file.lineCount = Some(line.content.toInt)
      case Code.File.EndInformation =>
      case Code.Issue.UniqueIdentifier => this.parseIssue(line.content, lines)
    }
  }

  def parseIssue(identifier: String, lines: Iterator[Line]) = {
    for (line <- lines.takeWhile(_.code != Code.Issue.End)) line.code match {
      case Code.Item.UniqueIdentifier => this.parseItem(line.content, lines)
      case _ => //System.err.println("Ignored from issue: " + line)
    }
  }

  def parseItem(identifier: String, lines: Iterator[Line]) = {
    for (line <- lines.takeWhile(_.code != Code.Item.End)) line.code match {
      case _ => //System.err.println("Ignored from item: " + line)
    }
  }

  case class Line(code: String, content: String)

  private def withLinesIterator[T](wokGzipPath: Path)(f: Iterator[Line] => T): T = {
    if (!wokGzipPath.isFile || !wokGzipPath.extension.exists(_ == "gz")) {
      throw new IllegalArgumentException("expected .gz file, found " + wokGzipPath.path)
    }
    wokGzipPath.inputStream.acquireAndGet {
      new GZIPInputStream(_).asInput.lines().withIterator { lines =>
        f(for (line <- lines) yield new Line(line.substring(0, 2), line.substring(3)))
      }
    }
  }

  case class File(
    var name: Option[String] = None,
    var copyright: Option[String] = None,
    var headerFileType: Option[String] = None,
    var creationDate: Option[String] = None,
    var version: Option[String] = None,
    var productCodes: Buffer[String] = Buffer.empty[String],
    var customizationDate: Option[String] = None,
    var productionWeeks: Option[String] = None,
    var periodicity: Option[String] = None,
    var issueCount: Option[Int] = None,
    var itemCount: Option[Int] = None,
    var lineCount: Option[Int] = None)

  object Code {
    // File information records
    object File {
      final val Name = "FN"
      final val Copyright = "CC"
      final val HeaderFileType = "HF"
      final val CreationDate = "H8"
      final val Version = "H9"
      final val ProductCode = "PV"
      final val CustomizationDate = "CW"
      final val ProductionWeeks = "PW"
      final val Periodicity = "FQ"
      final val IssueCount = "H5"
      final val ItemCount = "H6"
      final val LineCount = "H7"
      final val EndInformation = "RE"
      final val End = "EF"
    }

    // Source issue records
    object Issue {
      final val UniqueIdentifier = "UI"
      final val TransactionTag = "T1"
      final val AccessionNumber = "GA"
      final val SequenceNumber = "SQ"
      final val Type = "PT"
      final val FullTitle = "SO"
      final val ISOAbbreviatedTitle = "JI"
      final val Abbreviated11CharacterTitle = "J1"
      final val Abbreviated20CharacterTitle = "J2"
      final val Abbreviated29CharacterTitle = "J9"
      final val FullProductCoverage = "CF"
      final val SubjectCategory = "SC"
      final val ISSN = "SN"
      final val BookSeriesTitle = "SE"
      final val BookSeriesSubTitle = "BS"
      final val PublisherName = "PU"
      final val PublisherCity = "PI"
      final val PublisherAddress = "PA"
      final val Volume = "VL"
      final val Issue = "IS"
      final val PublicationYear = "PY"
      final val PublicationDate = "PD"
      final val Part = "PN"
      final val Supplement = "SU"
      final val SpecialIssue = "SI"
      final val TGAAvailability = "TV"
      final val SourceItemCount = "IL"
      final val FirstLoadDate = "LD"
      final val OrderNumber = "IO"
      final val End = "RE"
    }

    // Source item records
    object Item {
      final val UniqueIdentifier = "UT"
      final val TransactionTag = "T2"
      final val Identifier = "AR"
      final val Title = "TI"
      final val ReviewedWorkLanguage = "RL"
      final val ReviewedWorkAuthor = "RW"
      final val ReviewedWorkPublicationYear = "RY"
      final val Author = "AU"
      final val AuthorRole = "RO"
      final val AuthorLastName = "LN"
      final val AuthorFirstName = "AF"
      final val AuthorNameSuffix = "AS"
      final val AuthorAddressIdentifier = "AD"
      final val AuthorFullAddress = "AA"
      final val AuthorEmailAddress = "EM"
      final val CorporateAuthor = "AG"
      final val CorporateAuthorAddressIdentifier = "AD"
      final val CorporateAuthorFullAddress = "AA"
      final val CorporateAuthorEmailAddress = "EM"
      final val Type = "DT"
      final val BeginningPage = "BP"
      final val EndingPage = "EP"
      final val PageCount = "PG"
      final val Language = "LA"
      final val MeetingAbstractNumber = "MA"
      final val AuthorKeyword = "DE"
      object ReprintAddress {
        final val Begin = "RP"
        final val Author = "RA"
        final val FullAddress = "NF"
        final val Organization = "NC"
        final val SubOrganization = "ND"
        final val StreetAddress = "NN"
        final val City = "NY"
        final val Province = "NP"
        final val Country = "NU"
        final val PostalCode = "NZ"
        final val End = "EA"
      }
      object ResearchAddress {
        final val Begin = "C1"
        final val AddressIdentifier = "CN"
        final val FullAddress = "NF"
        final val Organization = "NC"
        final val SubOrganization = "ND"
        final val City = "NY"
        final val Province = "NP"
        final val Country = "NU"
        final val PostalCode = "NZ"
        final val End = "EA"
      }
      final val FundingAcknowledgementText = "GT"
      object FundingAcknowledgement {
        final val Begin = "GB"
        final val OrganizationName = "GO"
        final val GrantNumber = "GN"
        final val End = "GX"
      }
      final val AbstractAvailability = "AV"
      final val Abstract = "AB"
      final val CitedReferenceCount = "NR"
      object CitedPatent {
        final val Begin = "CP"
        final val Assignee = "/A"
        final val Year = "/Y"
        final val Number = "/W"
        final val Country = "/N"
        final val Type = "/C"
        final val End = "EC"
      }
      object CitedReference {
        final val Begin = "CR"
        final val UniqueIdentifier = "RS"
        final val Author = "/A"
        final val Year = "/Y"
        final val Work = "/W"
        final val Volume = "/V"
        final val Page = "/P"
        final val ImplicitCiteCode = "/I"
        final val End = "EC"
      }
      final val End = "EX"
    }
  }
}