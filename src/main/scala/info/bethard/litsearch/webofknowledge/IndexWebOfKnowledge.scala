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
      _.filter(_.code == "H8").map(_.content).next
    }
    for (path <- paths.sortBy(getDate)) {
      println(path.path)
      withLinesIterator(path) { lines =>
        val line = lines.next
        assert(line.code == "FN")
        this.parseFile(line.content, lines)
      }
    }
  }

  def parseFile(fileName: String, lines: Iterator[Line]) = {
    val file = new File(fileName)
    for (line <- lines.takeWhile(_.code != "EF")) line.code match {
      case "CC" => file.copyright = Some(line.content)
      case "HF" => file.headerFileType = Some(line.content)
      case "H8" => file.creationDate = Some(line.content)
      case "H9" => file.version = Some(line.content)
      case "PV" => file.productCodes += line.content
      case "CW" => file.customizationDate = Some(line.content)
      case "PW" => file.productionWeeks = Some(line.content)
      case "FQ" => file.periodicity = Some(line.content)
      case "H5" => file.issueCount = Some(line.content.toInt)
      case "H6" => file.itemCount = Some(line.content.toInt)
      case "H7" => file.lineCount = Some(line.content.toInt)
      case "RE" => // end of file attributes
      case "UI" => this.parseIssue(line.content, lines)
    }
  }

  def parseIssue(issueIdentifier: String, lines: Iterator[Line]) = {
    val issue = new Issue(issueIdentifier)
    var lastCode = ""
    for (line <- lines.takeWhile(_.code != "RE")) {
      line.code match {
        case "T1" => line.content.charAt(0) match {
          case 'N' => // no change
          case 'C' => // TODO: issue changed, but article records may not be included
          case 'D' => // TODO: delete entire issue and all articles
          case 'R' => // TODO: issue and all articles will be replaced
          case 'I' => // TODO: (undocumented) insert new issue maybe?
        }
        case "PW" => issue.productionWeeks = Some(line.content)
        case "GA" => issue.accessionNumber = Some(line.content)
        case "SQ" => issue.sequenceNumber = Some(line.content)
        case "PT" => issue.documentType = Some(line.content)
        case "SO" => issue.title.append(line.content)
        case "JI" => issue.titleISO.append(line.content)
        case "J1" => issue.title11Character = Some(line.content)
        case "J2" => issue.title20Character = Some(line.content)
        case "J9" => issue.title29Character = Some(line.content)
        case "CF" => issue.fullProductCoverage += line.content
        case "SL" => issue.selectiveProductCoverage += line.content
        case "SC" => issue.subjectCategories += line.content
        case "SN" => issue.issn = Some(line.content)
        case "SE" => issue.bookSeriesTitle.append(line.content)
        case "BS" => issue.bookSeriesSubTitle.append(line.content)
        case "PU" => issue.publisherName = Some(line.content)
        case "PI" => issue.publisherCity = Some(line.content)
        case "PA" => issue.publisherAddress.append(line.content)
        case "VL" => issue.volume = Some(line.content)
        case "IS" => issue.issue = Some(line.content)
        case "PY" => issue.publicationYear = Some(line.content.toInt)
        case "PD" => issue.publicationDate = Some(line.content)
        case "PN" => issue.part = Some(line.content)
        case "SU" => issue.supplement = Some(line.content)
        case "SI" => issue.specialIssue = Some(line.content)
        case "TV" => issue.tgaAvailability = Some(line.content)
        case "IL" => issue.itemCount = Some(line.content.toInt)
        case "LD" => issue.firstLoadDate = Some(line.content)
        case "IO" => issue.orderNumber = Some(line.content)
        case "--" => lastCode match {
          case "SO" => issue.title.append(line.content)
          case "JI" => issue.titleISO.append(line.content)
          case "SE" => issue.bookSeriesTitle.append(line.content)
          case "BS" => issue.bookSeriesSubTitle.append(line.content)
          case "PA" => issue.publisherAddress.append(line.content)
          case _ => throw new UnsupportedOperationException("%s %s".format(lastCode, line))
        }
        case "UT" => this.parseItem(line.content, lines)
      }
      if (line.code != "--") {
        lastCode = line.code
      }
    }
  }

  def parseItem(itemIdentifier: String, lines: Iterator[Line]) = {
    val item = new Item(itemIdentifier)
    var lastCode = ""
    for (line <- lines.takeWhile(_.code != "EX")) {
      line.code match {
        case "T2" => line.content.charAt(0) match {
          case 'R' => // TODO: replace or insert article
          case 'C' => // TODO: modify article
          case 'I' => // TODO: (undocumented) insert new item maybe?
        }
        case "T3" => // specifies what changed, but full record will be present
        case "AR" => item.identifierAlternate = Some(line.content)
        case "T9" => item.identifierForReferences = Some(line.content)
        case "SL" => item.selectiveProductCoverage += line.content
        case "TI" => item.title.append(line.content)
        case "RL" => item.reviewedWorkLanguages += line.content
        case "RW" => item.reviewedWorkAuthors += line.content
        case "RY" => item.reviewedWorkPublicationYear = Some(line.content.toInt)
        case "AU" => item.authors += line.content
        case "RO" => item.authorRoles += line.content
        case "LN" => item.authorLastNames += line.content
        case "AF" => item.authorFirstNames += line.content
        case "AS" => item.authorNameSuffixes += line.content
        case "AD" => item.authorAddressIdentifiers += line.content
        case "AA" => item.authorFullAddresses += new StringBuilder(line.content)
        case "EM" => item.authorEmailAddresses += new StringBuilder(line.content)
        case "AG" => item.corporateAuthor += line.content
        case "DT" => item.documentType = Some(line.content)
        case "BP" => item.beginningPage = Some(line.content)
        case "EP" => item.endingPage = Some(line.content)
        case "PG" => item.pageCount = Some(line.content.toInt)
        case "LA" => item.languages += line.content
        case "MA" => item.meetingAbstractNumber = Some(line.content)
        case "DE" => item.keywords += new StringBuilder(line.content)
        case "ID" => item.keywordsPlus += new StringBuilder(line.content)
        case "GT" => item.fundingAcknowledgementText += new StringBuilder(line.content)
        case "AV" => item.abstractAvailability = Some(line.content)
        case "AB" => item.abstractText += new StringBuilder(line.content)
        case "NR" => {
          val content = line.content.trim
          if (content != "NK") {
            item.citedReferenceCount = Some(content.toInt)
          }
        }
        case "--" => lastCode match {
          case "TI" => item.title.append(line.content)
          case "AA" => item.authorFullAddresses.last.append(line.content)
          case "EM" => item.authorEmailAddresses.last.append(line.content)
          case "DE" => item.keywords.last.append(line.content)
          case "ID" => item.keywordsPlus.last.append(line.content)
          case "GT" => item.fundingAcknowledgementText.last.append(line.content)
          case "AB" => item.abstractText.last.append(line.content)
        }
        case "RP" => this.parseReprintAddress(lines)
        case "C1" => this.parseResearchAddress(lines)
        case "GB" => this.parseFundingAcknowledgement(lines)
        case "CP" => this.parseCitedPatent(lines)
        case "CR" => this.parseCitedReference(lines)
      }
      if (line.code != "--") {
        lastCode = line.code
      }
    }
  }

  def parseReprintAddress(lines: Iterator[Line]) = {
    var lastCode = ""
    for (line <- lines.takeWhile(_.code != "EA")) {
      line.code match {
        case "RA" => // TODO: Author
        case "NF" => // TODO: FullAddress
        case "NC" => // TODO: Organization
        case "ND" => // TODO: SubOrganization
        case "NN" => // TODO: StreetAddress
        case "NY" => // TODO: City
        case "NP" => // TODO: Province
        case "NU" => // TODO: Country
        case "NZ" => // TODO: PostalCode
        case "--" => // TODO: continuation of previous code
      }
      if (line.code != "--") {
        lastCode = line.code
      }
    }
  }

  def parseResearchAddress(lines: Iterator[Line]) = {
    var lastCode = ""
    for (line <- lines.takeWhile(_.code != "EA")) {
      line.code match {
        case "CN" => // TODO: AddressIdentifier
        case "NF" => // TODO: FullAddress
        case "NC" => // TODO: Organization
        case "ND" => // TODO: SubOrganization
        case "NY" => // TODO: City
        case "NP" => // TODO: Province
        case "NU" => // TODO: Country
        case "NZ" => // TODO: PostalCode
        case "--" => // TODO: continuation of previous code
      }
      if (line.code != "--") {
        lastCode = line.code
      }
    }
  }

  def parseFundingAcknowledgement(lines: Iterator[Line]) = {
    var lastCode = ""
    for (line <- lines.takeWhile(_.code != "GX")) {
      line.code match {
        case "GO" => // TODO: OrganizationName
        case "GN" => // TODO: GrantNumber
        case "--" => // TODO: continuation of previous code
      }
      if (line.code != "--") {
        lastCode = line.code
      }
    }
  }

  def parseCitedPatent(lines: Iterator[Line]) = {
    var lastCode = ""
    for (line <- lines.takeWhile(_.code != "EC")) {
      line.code match {
        case "/A" => // TODO: Assignee
        case "/Y" => // TODO: Year
        case "/W" => // TODO: Number
        case "/N" => // TODO: Country
        case "/C" => // TODO: Type
      }
      if (line.code != "--") {
        lastCode = line.code
      }
    }
  }

  def parseCitedReference(lines: Iterator[Line]) = {
    var lastCode = ""
    for (line <- lines.takeWhile(_.code != "EC")) {
      line.code match {
        case "RS" => // TODO: Identifier
        case "R9" => // TODO: IdentifierForReferences
        case "/A" => // TODO: Author
        case "/Y" => // TODO: Year
        case "/W" => // TODO: Work
        case "/V" => // TODO: Volume
        case "/P" => // TODO: Page
        case "/I" => // TODO: ImplicitCiteCode
      }
      if (line.code != "--") {
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

  class File(val name: String) {
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
}