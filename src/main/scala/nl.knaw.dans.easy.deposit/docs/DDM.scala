/**
 * Copyright (C) 2018 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.easy.deposit.docs

import java.io.{ BufferedInputStream, ByteArrayInputStream }
import java.net.UnknownHostException
import java.nio.charset.StandardCharsets

import javax.xml.XMLConstants
import javax.xml.transform.Source
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.{ Schema, SchemaFactory }
import nl.knaw.dans.easy.deposit.docs.DatasetMetadata._
import nl.knaw.dans.easy.deposit.docs.JsonUtil.InvalidDocumentException
import nl.knaw.dans.easy.deposit.docs.dm.DateQualifier.DateQualifier
import nl.knaw.dans.easy.deposit.docs.dm._
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.xml.sax.SAXParseException
import resource.{ ManagedResource, Using }

import scala.util.{ Failure, Try }
import scala.xml._

object DDM extends DebugEnhancedLogging {
  val schemaNameSpace: String = "http://easy.dans.knaw.nl/schemas/md/ddm/"
  val schemaLocation: String = "https://easy.dans.knaw.nl/schemas/md/2017/09/ddm.xsd"

  def apply(dm: DatasetMetadata): Try[Elem] = Try {
    implicit val lang: Option[Attribute] = dm.languageOfDescription.map(l => new PrefixedAttribute("xml", "lang", l.key, Null))

    // validation like mustBeNonEmpty and mustHaveOne
    dm.doi.getOrElse(throwInvalidDocumentException(s"Please first GET a DOI for this deposit"))

    <ddm:DDM
      xmlns:dc="http://purl.org/dc/elements/1.1/"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xmlns:dcterms="http://purl.org/dc/terms/"
      xmlns:dcx-dai="http://easy.dans.knaw.nl/schemas/dcx/dai/"
      xmlns:dcx-gml="http://easy.dans.knaw.nl/schemas/dcx/gml/"
      xmlns:gml="http://www.opengis.net/gml"
      xmlns:ddm="http://easy.dans.knaw.nl/schemas/md/ddm/"
      xmlns:id-type="http://easy.dans.knaw.nl/schemas/vocab/identifier-type/"
      xsi:schemaLocation={s"$schemaNameSpace $schemaLocation"}
    >
      <ddm:profile>
        { dm.titles.getNonEmpty.asNodes("dc:title").addAttr(lang).mustBeNonEmpty("a title") }
        { dm.descriptions.getNonEmpty.asNodes("dcterms:description").addAttr(lang).mustBeNonEmpty("a description") }
        { dm.creatorsWithoutRights.asNodes("dcx-dai:creatorDetails", details).mustBeNonEmpty("a creator") }
        { dm.datesCreated.asNodes("ddm:created", _.value).mustHaveOne(DateQualifier.dateSubmitted) }
        { dm.datesAvailable.asNodes("ddm:available", _.value).mustHaveOne(DateQualifier.available) }
        { dm.audiences.getNonEmpty.asNodes("ddm:audience", _.key).mustBeNonEmpty("an audience") }
        { dm.accessRights.toSeq.asNodes("ddm:accessRights", _.category.toString).mustBeNonEmpty("the accessRights") }
      </ddm:profile>
      <ddm:dcmiMetadata>
        { dm.allIdentifiers.asNodes("dcterms:identifier", _.value, ("xsi:type", _.scheme)) }
        { dm.alternativeTitles.getNonEmpty.asNodes("dcterms:alternative").addAttr(lang) }
        { dm.relations.getNonEmpty.map(src => details(src.withCleanOptions)) }
        { dm.contributorsWithoutRights.asNodes("dcx-dai:contributorDetails", details) }
        { dm.rightsHolders.asNodes("dcterms:rightsHolder", _.toString) }
        { dm.publishers.getNonEmpty.asNodes("dcterms:publisher").addAttr(lang) }
        { dm.sources.getNonEmpty.asNodes("dc:source").addAttr(lang) }
        { dm.allTypes.asNodes("dcterms:type", _.value, ("xsi:type", _.schemeAsString)) }
        { dm.formats.getNonEmpty.asNodes("dcterms:format", _.value, ("xsi:type", _.schemeAsString)) }
        { dm.otherDates.toNodes(_.qualifier.toString, _.value, ("xsi:type", _.schemeAsString)) }
        { dm.spatialPoints.getNonEmpty.asNodes("dcx-gml:spatial", details, ("srsName", _.srsName)) }
        { dm.spatialBoxes.getNonEmpty.asNodes("dcx-gml:spatial", details) }
        { dm.license.getNonEmpty.asNodes("dcterms:license") /* xsi:type="dcterms:URI" not supported by json */ }
      </ddm:dcmiMetadata>
    </ddm:DDM>
  }.flatMap(validate)

  private def details(point: SpatialPoint) = {
    <Point xmlns="http://www.opengis.net/gml">
        <pos>{ point.pos }</pos>
    </Point>
  }

  private def details(box: SpatialBox) = {
    <boundedBy xmlns="http://www.opengis.net/gml">
        <Envelope srsName={ box.srsName }>
            <lowerCorner>{ box.lower }</lowerCorner>
            <upperCorner>{ box.upper }</upperCorner>
        </Envelope>
    </boundedBy>
  }

  private def details(relation: RelationType)
                     (implicit lang: Option[Attribute]) = {
    (relation match {
      case Relation(_, Some(url: String), Some(title: String)) => <x href={ url }>{ title }</x>.addAttr(lang)
      case Relation(_, Some(url: String), None) => <x href={ url }>{ url }</x>
      case Relation(_, None, Some(title: String)) => <x>{ title }</x>.addAttr(lang)
      case relatedID: RelatedIdentifier => <x  xsi:type={ relatedID.schemeAsString }>{ relatedID.value }</x>
    }) withLabel (relation match {
      case Relation(_, None, _) | RelatedIdentifier(_, _, _) => relation.qualifier.toString
      case Relation(qualifier, Some(_), _) => qualifier.toString.replace("dcterms", "ddm")
    })
  }

  private def details(author: Author)
                     (implicit lang: Option[Attribute]) = {
    if (author.surname.isEmpty)
      author.organization.toSeq.map(orgDetails(author.role))
    else // TODO ids
      <dcx-dai:author>
        { author.titles.getNonEmpty.asNodes("dcx-dai:titles").addAttr(lang) }
        { author.initials.getNonEmpty.asNodes("dcx-dai:initials") }
        { author.insertions.getNonEmpty.asNodes("dcx-dai:insertions") }
        { author.surname.getNonEmpty.asNodes("dcx-dai:surname") }
        { author.role.toSeq.asNodes("dcx-dai:role", _.key) }
        { author.organization.getNonEmpty.map(orgDetails()) }
      </dcx-dai:author>
  }

  private def orgDetails(role: Option[SchemedKeyValue] = None)
                        (organization: String)
                        (implicit lang: Option[Attribute]) = {
      <dcx-dai:organization>
        { role.toSeq.map(role => <dcx-dai:role>{ role.key }</dcx-dai:role>) }
        { <dcx-dai:name>{ organization }</dcx-dai:name>.addAttr(lang) }
      </dcx-dai:organization>
  }

  /** @param elem XML element to be adjusted */
  implicit class RichElem(val elem: Elem) extends AnyVal {

    /** @param str the desired tag (namespace:label) or (label) */
    @throws[InvalidDocumentException]("when str is not a valid XML label (has more than one ':')")
    def withLabel(str: String): Elem = {
      str.split(":") match {
        case Array(label) => elem.copy(label = label)
        case Array(prefix, label) => elem.copy(prefix = prefix, label = label)
        case a => throwInvalidDocumentException(
          s"expecting (label) or (prefix:label); got [${ a.mkString(":") }] to adjust the <key> of ${ Utility.trim(elem) }"
        )
      }
    }

    def addAttr(lang: Option[Attribute]): Elem = lang.map(elem % _).getOrElse(elem)
  }

  /** @param elems the sequence of XML elements to adjust */
  private implicit class RichElems(val elems: Seq[Elem]) extends AnyVal {
    def addAttr(lang: Option[Attribute]): Seq[Elem] = elems.map(_.addAttr(lang))

    def withLabel(str: String): Seq[Elem] = elems.map(_.copy(label = str))

    def mustBeNonEmpty(str: String): Seq[Elem] = {
      if (elems.isEmpty) throw missingValue(str)
      else elems
    }

    def mustHaveOne(dateQualifier: DateQualifier): Seq[Elem] = elems match {
      case Seq() => throw missingValue(dateQualifier.toString)
      case Seq(_) => elems
      case _ => throwInvalidDocumentException(s"Just one $dateQualifier allowed")
    }
  }

  private implicit class AsNodes[T](val ts: Seq[T]) extends AnyVal {
    def asNodes(label: String): Seq[Elem] = asNodes(label, identity)

    def asNodes[S](label: String, f: T => S): Seq[Elem] = {
      ts.map(t => <key>{f(t)}</key> withLabel label)
    }

    def asNodes[S](label: String, valueGenerator: T => S,
                   attribute: (String, T => String)): Seq[Elem] = {
      val (attributeName, attributeGenerator) = attribute

      val attr = createAttribute(attributeName, attributeGenerator)
      ts.map(t => <key>{valueGenerator(t)}</key> withLabel label copy (attributes = attr(t)))
    }

    def toNodes[S](labelGenerator: T => String, valueGenerator: T => S,
                   attribute: (String, T => String)): Seq[Elem] = {
      val (attributeName, attributeGenerator) = attribute

      val attr = createAttribute(attributeName, attributeGenerator)
      ts.map(t => <key>{valueGenerator(t)}</key> withLabel labelGenerator(t) copy (attributes = attr(t)))
    }

    @throws[InvalidDocumentException]("when str is not a valid attribute name (has more than one ':')")
    private def createAttribute(attrName: String, valueGenerator: T => String): T => Attribute = {
      attrName.split(":") match {
        case Array(name) => (t: T) => new UnprefixedAttribute(name, valueGenerator(t), Null)
        case Array(prefix, name) => (t: T) => new PrefixedAttribute(prefix, name, valueGenerator(t), Null)
        case a => throwInvalidDocumentException(
          s"expecting (attributeName) or (prefix:attributeName); got [${ a.mkString(":") }] }"
        )
      }
    }
  }

  private implicit class OptionSeq[T](val sources: Option[Seq[T]]) extends AnyVal {
    def getNonEmpty: Seq[T] = sources.map(_.filterNot {
      case source: String => source.trim.isEmpty
      case _ => false
    }).getOrElse(Seq.empty)
  }

  private implicit class RichOption[T](val sources: Option[T]) extends AnyVal {
    def getNonEmpty: Seq[T] = sources.toSeq.filterNot {
      case source: String => source.trim.isEmpty
      case _ => false
    }
  }

  // pretty provides friendly trouble shooting for complex XML's
  private val prettyPrinter: PrettyPrinter = new scala.xml.PrettyPrinter(1024, 2)

  lazy val triedSchema: Try[Schema] = Try { // loading postponed until we actually start validating
    SchemaFactory
      .newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
      .newSchema(Array(new StreamSource(schemaLocation)).toArray[Source])
  }

  private def validate(ddm: Elem): Try[Elem] = {
    logger.debug(prettyPrinter.format(ddm))
    triedSchema.map(schema =>
      managedInputStream(ddm)
        .apply(inputStream => schema
          .newValidator()
          .validate(new StreamSource(inputStream))
        )
    )
      .map(_ => ddm)
      .recoverWith {
        case e: SAXParseException if e.getCause.isInstanceOf[UnknownHostException] =>
          logger.error(e.getMessage, e)
          Failure(SchemaNotAvailableException(e))
        case e: SAXParseException => Failure(invalidDatasetMetadataException(e))
        case e => Failure(e)
      }
  }

  private def managedInputStream(ddm: Elem): ManagedResource[BufferedInputStream] = {
    Using.bufferedInputStream(
      new ByteArrayInputStream(
        prettyPrinter
          .format(ddm)
          .getBytes(StandardCharsets.UTF_8)
      ))
  }

  private def throwInvalidDocumentException(msg: String) = {
    throw invalidDatasetMetadataException(new Exception(msg))
  }

  private def invalidDatasetMetadataException(exception: Exception) = {
    InvalidDocumentException("DatasetMetadata", exception)
  }

  case class SchemaNotAvailableException(t: Throwable = null)
    extends Exception(s"Schema's for validation not available, please try again later.", t)
}
