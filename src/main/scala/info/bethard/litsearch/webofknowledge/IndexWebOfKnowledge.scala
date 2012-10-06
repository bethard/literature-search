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
    val issue = new Issue
    var lastCode = ""
    for (line <- lines.takeWhile(_.code != Code.Issue.End)) {
      line.code match {
        case Code.Issue.UniqueIdentifier => issue.uniqueIdentifier = Some(line.content)
        case Code.Issue.TransactionTag => issue.transactionTag = Some(line.content)
        case Code.Issue.ProductionWeeks => issue.productionWeeks = Some(line.content)
        case Code.Issue.AccessionNumber => issue.accessionNumber = Some(line.content)
        case Code.Issue.SequenceNumber => issue.sequenceNumber = Some(line.content)
        case Code.Issue.DocumentType => issue.documentType = Some(line.content)
        case Code.Issue.Title => issue.title.append(line.content)
        case Code.Issue.TitleISO => issue.titleISO.append(line.content)
        case Code.Issue.Title11Character => issue.title11Character = Some(line.content)
        case Code.Issue.Title20Character => issue.title20Character = Some(line.content)
        case Code.Issue.Title29Character => issue.title29Character = Some(line.content)
        case Code.Issue.FullProductCoverage => issue.fullProductCoverage += line.content
        case Code.Issue.SelectiveProductCoverage => issue.selectiveProductCoverage += line.content
        case Code.Issue.SubjectCategory => issue.subjectCategories += line.content
        case Code.Issue.ISSN => issue.issn = Some(line.content)
        case Code.Issue.BookSeriesTitle => issue.bookSeriesTitle = Some(line.content)
        case Code.Issue.BookSeriesSubTitle => issue.bookSeriesSubTitle = Some(line.content)
        case Code.Issue.PublisherName => issue.publisherName = Some(line.content)
        case Code.Issue.PublisherCity => issue.publisherCity = Some(line.content)
        case Code.Issue.PublisherAddress => issue.publisherAddress.append(line.content)
        case Code.Issue.Volume => issue.volume = Some(line.content)
        case Code.Issue.Issue => issue.issue = Some(line.content)
        case Code.Issue.PublicationYear => issue.publicationYear = Some(line.content.toInt)
        case Code.Issue.PublicationDate => issue.publicationDate = Some(line.content)
        case Code.Issue.Part => issue.part = Some(line.content)
        case Code.Issue.Supplement => issue.supplement = Some(line.content)
        case Code.Issue.SpecialIssue => issue.specialIssue = Some(line.content)
        case Code.Issue.TGAAvailability => issue.tgaAvailability = Some(line.content)
        case Code.Issue.ItemCount => issue.itemCount = Some(line.content.toInt)
        case Code.Issue.FirstLoadDate => issue.firstLoadDate = Some(line.content)
        case Code.Issue.OrderNumber => issue.orderNumber = Some(line.content)
        case Code.Continuation => lastCode match {
          case Code.Issue.Title => issue.title.append(line.content)
          case Code.Issue.TitleISO => issue.titleISO.append(line.content)
          case Code.Issue.PublisherAddress => issue.publisherAddress.append(line.content)
          case _ => throw new UnsupportedOperationException("%s %s".format(lastCode, line))
        }
        case Code.Item.UniqueIdentifier => this.parseItem(line.content, lines)
      }
      if (line.code != Code.Continuation) {
        lastCode = line.code
      }
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

  private final val NoString: Option[String] = None
  private final val NoInt: Option[Int] = None

  class File {
    var name = NoString
    var copyright = NoString
    var headerFileType = NoString
    var creationDate = NoString
    var version = NoString
    var productCodes = Buffer.empty[String]
    var customizationDate = NoString
    var productionWeeks = NoString
    var periodicity = NoString
    var issueCount = NoInt
    var itemCount = NoInt
    var lineCount = NoInt
  }

  class Issue {
    var uniqueIdentifier = NoString
    var transactionTag = NoString
    var productionWeeks = NoString
    var accessionNumber = NoString
    var sequenceNumber = NoString
    var documentType = NoString
    var title = new StringBuilder
    var titleISO = new StringBuilder
    var title11Character = NoString
    var title20Character = NoString
    var title29Character = NoString
    var fullProductCoverage = Buffer.empty[String]
    var selectiveProductCoverage = Buffer.empty[String]
    var subjectCategories = Buffer.empty[String]
    var issn = NoString
    var bookSeriesTitle = NoString
    var bookSeriesSubTitle = NoString
    var publisherName = NoString
    var publisherCity = NoString
    var publisherAddress = new StringBuilder
    var volume = NoString
    var issue = NoString
    var publicationYear = NoInt
    var publicationDate = NoString
    var part = NoString
    var supplement = NoString
    var specialIssue = NoString
    var tgaAvailability = NoString
    var itemCount = NoInt
    var firstLoadDate = NoString
    var orderNumber = NoString
  }

  object Code {
    final val Continuation = "--"

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
      final val ProductionWeeks = "PW"
      final val AccessionNumber = "GA"
      final val SequenceNumber = "SQ"
      final val DocumentType = "PT"
      final val Title = "SO"
      final val TitleISO = "JI"
      final val Title11Character = "J1"
      final val Title20Character = "J2"
      final val Title29Character = "J9"
      final val FullProductCoverage = "CF"
      final val SelectiveProductCoverage = "SL"
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
      final val ItemCount = "IL"
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
      final val DocumentType = "DT"
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