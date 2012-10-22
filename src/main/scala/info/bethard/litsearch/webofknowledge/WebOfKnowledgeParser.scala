package info.bethard.litsearch.webofknowledge

import java.util.zip.GZIPInputStream
import scala.collection.mutable.Buffer
import scalax.file.Path
import scalax.io.JavaConverters.asInputConverter
import java.util.logging.Logger

class WebOfKnowledgeParser {
  import WebOfKnowledgeParser._

  private val logger = Logger.getLogger(this.getClass.getName)

  def parse(directories: Seq[Path]) = {
    // get all files in the directories, and sort them by date
    val paths = directories.flatMap(_.children()).sortBy {
      withLinesIterator(_) {
        _.filter(_.code == "H8").map(_.content).next
      }
    }

    // parse each file
    for (path <- paths) {
      withLinesIterator(path) { lines =>
        val line = lines.next
        assert(line.code == "FN")
        this.parseFile(new File(path, line.content), lines)
      }
    }
  }

  protected def beginFile(file: File) = {}

  def parseFile(file: File, lines: Iterator[Line]) = {
    this.beginFile(file)
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
      case "UI" => this.parseIssue(new Issue(file, line.content), lines)
    }
    this.endFile(file)
  }

  protected def endFile(file: File) = {}

  def parseIssue(issue: Issue, lines: Iterator[Line]) = {
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
        case "UT" => this.parseItem(new Item(issue, line.content), lines)
      }
      if (line.code != "--") {
        lastCode = line.code
      }
    }
  }

  protected def beginItem(item: Item) = {}

  def parseItem(item: Item, lines: Iterator[Line]) = {
    this.beginItem(item)
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
        case "RY" => item.reviewedWorkPublicationYear =
          try {
            Some(line.content.toInt)
          } catch {
            case e: NumberFormatException => {
              val message = "Skipping invalid reviewed work publication year '%s'"
              this.logger.warning(message.format(line.content))
              None
            }
          }
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
        case "ET" => lastCode match {
          case "TI" => item.title.append(line.content)
        }
        case "RP" => item.reprintAddresses += this.parseReprintAddress(lines)
        case "C1" => item.researchAddresses += this.parseResearchAddress(lines)
        case "GB" => item.fundingAcknowledgements += this.parseFundingAcknowledgement(lines)
        case "CP" => item.citedPatents += this.parseCitedPatent(lines)
        case "CR" => item.citedReferences += this.parseCitedReference(lines)
      }
      if (line.code != "--" && line.code != "ET") {
        lastCode = line.code
      }
    }
    this.endItem(item)
  }

  protected def endItem(item: Item) = {}

  def parseReprintAddress(lines: Iterator[Line]): ReprintAddress = {
    val address = new ReprintAddress
    var lastCode = ""
    for (line <- lines.takeWhile(_.code != "EA")) {
      line.code match {
        case "RA" => address.author = Some(line.content)
        case "NF" => address.fullAddress.append(line.content)
        case "NC" => address.organization.append(line.content)
        case "ND" => address.subOrganizations += line.content
        case "NN" => address.streetAddress.append(line.content)
        case "NY" => address.city = Some(line.content)
        case "NP" => address.province = Some(line.content)
        case "NU" => address.country = Some(line.content)
        case "NZ" => address.postalCodes.append(line.content)
        case "--" => lastCode match {
          case "NF" => address.fullAddress.append(line.content)
          case "NC" => address.organization.append(line.content)
          case "NN" => address.streetAddress.append(line.content)
        }
      }
      if (line.code != "--") {
        lastCode = line.code
      }
    }
    address
  }

  def parseResearchAddress(lines: Iterator[Line]): ResearchAddress = {
    val address = new ResearchAddress
    var lastCode = ""
    for (line <- lines.takeWhile(_.code != "EA")) {
      line.code match {
        case "CN" => address.identifier = Some(line.content)
        case "NF" => address.fullAddress.append(line.content)
        case "NC" => address.organization.append(line.content)
        case "ND" => address.subOrganizations += line.content
        case "NN" => address.streetAddress.append(line.content)
        case "NY" => address.city = Some(line.content)
        case "NP" => address.province = Some(line.content)
        case "NU" => address.country = Some(line.content)
        case "NZ" => address.postalCodes.append(line.content)
        case "--" => lastCode match {
          case "NF" => address.fullAddress.append(line.content)
          case "NC" => address.organization.append(line.content)
          case "NN" => address.streetAddress.append(line.content)
        }
      }
      if (line.code != "--") {
        lastCode = line.code
      }
    }
    address
  }

  def parseFundingAcknowledgement(lines: Iterator[Line]): FundingAcknowledgement = {
    val acknowledgement = new FundingAcknowledgement
    var lastCode = ""
    for (line <- lines.takeWhile(_.code != "GX")) {
      line.code match {
        case "GO" => acknowledgement.organizationName.append(line.content)
        case "GN" => acknowledgement.grantNumbers += new StringBuilder(line.content)
        case "--" => lastCode match {
          case "GO" => acknowledgement.organizationName.append(line.content)
          case "GN" => acknowledgement.grantNumbers.last.append(line.content)
        }
      }
      if (line.code != "--") {
        lastCode = line.code
      }
    }
    acknowledgement
  }

  def parseCitedPatent(lines: Iterator[Line]): CitedPatent = {
    val citation = new CitedPatent
    for (line <- lines.takeWhile(_.code != "EC")) line.code match {
      case "/A" => citation.assignee = Some(line.content)
      case "/Y" => citation.year = Some(line.content.toInt)
      case "/W" => citation.number = Some(line.content)
      case "/N" => citation.country = Some(line.content)
      case "/C" => citation.patentType = Some(line.content)
    }
    citation
  }

  def parseCitedReference(lines: Iterator[Line]): CitedReference = {
    val citation = new CitedReference
    for (line <- lines.takeWhile(_.code != "EC"); if line.content != "") line.code match {
      case "RS" => citation.identifier = Some(line.content)
      case "R9" => citation.identifierForReferences = Some(line.content)
      case "/A" => citation.author = Some(line.content)
      case "/Y" => citation.year = Some(line.content)
      case "/W" => citation.work = Some(line.content)
      case "/V" => citation.volume = Some(line.content)
      case "/P" => citation.page = Some(line.content)
      case "/I" => citation.citationType = Some(line.content)
    }
    citation
  }
}

object WebOfKnowledgeParser {
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

  class File(val path: Path, val name: String) {
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

  class Issue(val file: File, val identifier: String) {
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

  class Item(val issue: Issue, val identifier: String) {
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
    var reprintAddresses = Buffer.empty[ReprintAddress]
    var researchAddresses = Buffer.empty[ResearchAddress]
    var fundingAcknowledgements = Buffer.empty[FundingAcknowledgement]
    var citedPatents = Buffer.empty[CitedPatent]
    var citedReferences = Buffer.empty[CitedReference]
  }

  class ReprintAddress {
    var author = NoString
    var fullAddress = new StringBuilder
    var organization = new StringBuilder
    var subOrganizations = Buffer.empty[String]
    var streetAddress = new StringBuilder
    var city = NoString
    var province = NoString
    var country = NoString
    var postalCodes = Buffer.empty[String]
  }

  class ResearchAddress {
    var identifier = NoString
    var fullAddress = new StringBuilder
    var organization = new StringBuilder
    var subOrganizations = Buffer.empty[String]
    var streetAddress = new StringBuilder
    var city = NoString
    var province = NoString
    var country = NoString
    var postalCodes = Buffer.empty[String]
  }

  class FundingAcknowledgement {
    var organizationName = new StringBuilder
    var grantNumbers = Buffer.empty[StringBuilder]
  }

  class CitedPatent {
    var assignee = NoString
    var year = NoInt
    var number = NoString
    var country = NoString
    var patentType = NoString
  }

  class CitedReference {
    var identifier = NoString
    var identifierForReferences = NoString
    var author = NoString
    var year = NoString // there are things like 19AU
    var work = NoString
    var volume = NoString
    var page = NoString
    var citationType = NoString
  }

  def main(args: Array[String]) = {
    if (args.length < 1) {
      val message = "usage: java %s wok-dir [wok-dir ...]"
      throw new IllegalArgumentException(message.format(this.getClass.getName))
    }
    val parser = new WebOfKnowledgeParser
    parser.parse(args.map(Path.fromString))
  }
}