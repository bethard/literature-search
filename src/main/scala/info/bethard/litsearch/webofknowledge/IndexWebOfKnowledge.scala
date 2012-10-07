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
      case Code.Issue.Identifier => this.parseIssue(line.content, lines)
    }
  }

  def parseIssue(issueIdentifier: String, lines: Iterator[Line]) = {
    val issue = new Issue(issueIdentifier)
    var lastCode = ""
    for (line <- lines.takeWhile(_.code != Code.Issue.End)) {
      line.code match {
        case Code.Issue.TransactionTag => line.content.charAt(0) match {
          case 'N' => // no change
          case 'C' => // TODO: issue changed, but article records may not be included
          case 'D' => // TODO: delete entire issue and all articles
          case 'R' => // TODO: issue and all articles will be replaced
          case 'I' => // TODO: (undocumented) insert new issue maybe?
        }
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
        case Code.Issue.BookSeriesTitle => issue.bookSeriesTitle.append(line.content)
        case Code.Issue.BookSeriesSubTitle => issue.bookSeriesSubTitle.append(line.content)
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
          case Code.Issue.BookSeriesTitle => issue.bookSeriesTitle.append(line.content)
          case Code.Issue.BookSeriesSubTitle => issue.bookSeriesSubTitle.append(line.content)
          case Code.Issue.PublisherAddress => issue.publisherAddress.append(line.content)
          case _ => throw new UnsupportedOperationException("%s %s".format(lastCode, line))
        }
        case Code.Item.Identifier => this.parseItem(line.content, lines)
      }
      if (line.code != Code.Continuation) {
        lastCode = line.code
      }
    }
  }

  def parseItem(itemIdentifier: String, lines: Iterator[Line]) = {
    val item = new Item(itemIdentifier)
    var lastCode = ""
    for (line <- lines.takeWhile(_.code != Code.Item.End)) {
      line.code match {
        case Code.Item.TransactionTag => line.content.charAt(0) match {
          case 'R' => // TODO: replace or insert article
          case 'C' => // TODO: modify article
          case 'I' => // TODO: (undocumented) insert new item maybe?
        }
        case Code.Item.TransactionChangeTag => // specifies what changed, but full record will be present
        case Code.Item.IdentifierAlternate => item.identifierAlternate = Some(line.content)
        case Code.Item.IdentifierForReferences => item.identifierForReferences = Some(line.content)
        case Code.Item.SelectiveProductCoverage => item.selectiveProductCoverage += line.content
        case Code.Item.Title => item.title.append(line.content)
        case Code.Item.ReviewedWorkLanguage => item.reviewedWorkLanguages += line.content
        case Code.Item.ReviewedWorkAuthor => item.reviewedWorkAuthors += line.content
        case Code.Item.ReviewedWorkPublicationYear => item.reviewedWorkPublicationYear = Some(line.content.toInt)
        case Code.Item.Author => item.authors += line.content
        case Code.Item.AuthorRole => item.authorRoles += line.content
        case Code.Item.AuthorLastName => item.authorLastNames += line.content
        case Code.Item.AuthorFirstName => item.authorFirstNames += line.content
        case Code.Item.AuthorNameSuffix => item.authorNameSuffixes += line.content
        case Code.Item.AuthorAddressIdentifier => item.authorAddressIdentifiers += line.content
        case Code.Item.AuthorFullAddress => item.authorFullAddresses += new StringBuilder(line.content)
        case Code.Item.AuthorEmailAddress => item.authorFullAddresses += new StringBuilder(line.content)
        case Code.Item.CorporateAuthor => item.corporateAuthor += line.content
        case Code.Item.DocumentType => item.documentType = Some(line.content)
        case Code.Item.BeginningPage => item.beginningPage = Some(line.content)
        case Code.Item.EndingPage => item.endingPage = Some(line.content)
        case Code.Item.PageCount => item.pageCount = Some(line.content.toInt)
        case Code.Item.Language => item.languages += line.content
        case Code.Item.MeetingAbstractNumber => item.meetingAbstractNumber = Some(line.content)
        case Code.Item.Keywords => item.keywords += new StringBuilder(line.content)
        case Code.Item.KeywordsPlus => item.keywordsPlus += new StringBuilder(line.content)
        case Code.Item.FundingAcknowledgementText => item.fundingAcknowledgementText += new StringBuilder(line.content)
        case Code.Item.AbstractAvailability => item.abstractAvailability = Some(line.content)
        case Code.Item.AbstractText => item.abstractText += new StringBuilder(line.content)
        case Code.Item.CitedReferenceCount => {
          val content = line.content.trim
          if (content != "NK") {
            item.citedReferenceCount = Some(content.toInt)
          }
        }
        case Code.Continuation => lastCode match {
          case Code.Item.Title => item.title.append(line.content)
          case Code.Item.AuthorFullAddress => item.authorFullAddresses.last.append(line.content)
          case Code.Item.AuthorEmailAddress => item.authorFullAddresses.last.append(line.content)
          case Code.Item.Keywords => item.keywords.last.append(line.content)
          case Code.Item.KeywordsPlus => item.keywordsPlus.last.append(line.content)
          case Code.Item.FundingAcknowledgementText => item.fundingAcknowledgementText.last.append(line.content)
          case Code.Item.AbstractText => item.abstractText.last.append(line.content)
        }
        case Code.ReprintAddress.Begin => this.parseReprintAddress(lines)
        case Code.ResearchAddress.Begin => this.parseResearchAddress(lines)
        case Code.FundingAcknowledgement.Begin => this.parseFundingAcknowledgement(lines)
        case Code.CitedPatent.Begin => this.parseCitedPatent(lines)
        case Code.CitedReference.Begin => this.parseCitedReference(lines)
      }
      if (line.code != Code.Continuation) {
        lastCode = line.code
      }
    }
  }

  def parseReprintAddress(lines: Iterator[Line]) = {
    var lastCode = ""
    for (line <- lines.takeWhile(_.code != Code.ReprintAddress.End)) {
      line.code match {
        case _ => // TODO
      }
      if (line.code != Code.Continuation) {
        lastCode = line.code
      }
    }
  }

  def parseResearchAddress(lines: Iterator[Line]) = {
    var lastCode = ""
    for (line <- lines.takeWhile(_.code != Code.ResearchAddress.End)) {
      line.code match {
        case _ => // TODO
      }
      if (line.code != Code.Continuation) {
        lastCode = line.code
      }
    }
  }

  def parseFundingAcknowledgement(lines: Iterator[Line]) = {
    var lastCode = ""
    for (line <- lines.takeWhile(_.code != Code.FundingAcknowledgement.End)) {
      line.code match {
        case _ => // TODO
      }
      if (line.code != Code.Continuation) {
        lastCode = line.code
      }
    }
  }

  def parseCitedPatent(lines: Iterator[Line]) = {
    var lastCode = ""
    for (line <- lines.takeWhile(_.code != Code.CitedPatent.End)) {
      line.code match {
        case _ => // TODO
      }
      if (line.code != Code.Continuation) {
        lastCode = line.code
      }
    }
  }

  def parseCitedReference(lines: Iterator[Line]) = {
    var lastCode = ""
    for (line <- lines.takeWhile(_.code != Code.CitedReference.End)) {
      line.code match {
        case _ => // TODO
      }
      if (line.code != Code.Continuation) {
        lastCode = line.code
      }
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

  class Issue(val identifier: String) {
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
    var bookSeriesTitle = new StringBuilder
    var bookSeriesSubTitle = new StringBuilder
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

  class Item(val identifier: String) {
    var identifierAlternate = NoString
    var identifierForReferences = NoString
    var selectiveProductCoverage = Buffer.empty[String]
    var title = new StringBuilder
    var reviewedWorkLanguages = Buffer.empty[String]
    var reviewedWorkAuthors = Buffer.empty[String]
    var reviewedWorkPublicationYear = NoInt
    var authors = Buffer.empty[String]
    var authorRoles = Buffer.empty[String]
    var authorLastNames = Buffer.empty[String]
    var authorFirstNames = Buffer.empty[String]
    var authorNameSuffixes = Buffer.empty[String]
    var authorAddressIdentifiers = Buffer.empty[String]
    var authorFullAddresses = Buffer.empty[StringBuilder]
    var authorEmailAddresses = Buffer.empty[StringBuilder]
    var corporateAuthor = Buffer.empty[String]
    var documentType = NoString
    var beginningPage = NoString
    var endingPage = NoString
    var pageCount = NoInt
    var languages = Buffer.empty[String]
    var meetingAbstractNumber = NoString
    var keywords = Buffer.empty[StringBuilder]
    var keywordsPlus = Buffer.empty[StringBuilder]
    var fundingAcknowledgementText = Buffer.empty[StringBuilder]
    var abstractAvailability = NoString
    var abstractText = Buffer.empty[StringBuilder]
    var citedReferenceCount = NoInt
  }

  object Code {
    final val Continuation = "--"

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

    object Issue {
      final val Identifier = "UI"
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

    object Item {
      final val Identifier = "UT"
      final val IdentifierAlternate = "AR"
      final val IdentifierForReferences = "T9"
      final val SelectiveProductCoverage = "SL"
      final val TransactionTag = "T2"
      final val TransactionChangeTag = "T3"
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
      final val DocumentType = "DT"
      final val BeginningPage = "BP"
      final val EndingPage = "EP"
      final val PageCount = "PG"
      final val Language = "LA"
      final val MeetingAbstractNumber = "MA"
      final val Keywords = "DE"
      final val KeywordsPlus = "ID"
      final val FundingAcknowledgementText = "GT"
      final val AbstractAvailability = "AV"
      final val AbstractText = "AB"
      final val CitedReferenceCount = "NR"
      final val End = "EX"
    }

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

    object FundingAcknowledgement {
      final val Begin = "GB"
      final val OrganizationName = "GO"
      final val GrantNumber = "GN"
      final val End = "GX"
    }

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
      final val Identifier = "RS"
      final val IdentifierForReferences = "R9"
      final val Author = "/A"
      final val Year = "/Y"
      final val Work = "/W"
      final val Volume = "/V"
      final val Page = "/P"
      final val ImplicitCiteCode = "/I"
      final val End = "EC"
    }
  }
}