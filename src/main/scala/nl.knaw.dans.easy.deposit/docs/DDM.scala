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

import nl.knaw.dans.easy.deposit.Errors.InvalidDocumentException
import nl.knaw.dans.easy.deposit.docs.CollectionUtils._
import nl.knaw.dans.easy.deposit.docs.dm._
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import nl.knaw.dans.lib.string._

import scala.util.{ Failure, Try }
import scala.xml._

object DDM extends SchemedXml with DebugEnhancedLogging {
  override val schemaNameSpace: String = "http://easy.dans.knaw.nl/schemas/md/ddm/"
  override val schemaLocation: String = "https://easy.dans.knaw.nl/schemas/md/ddm/ddm.xsd"

  def apply(dm: DatasetMetadata): Try[Elem] = Try {
    val lang: String = dm.languageOfDescription.flatMap(_.key).nonBlankOrNull
    <ddm:DDM
      xmlns:dc="http://purl.org/dc/elements/1.1/"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xmlns:dcterms="http://purl.org/dc/terms/"
      xmlns:dcx-dai="http://easy.dans.knaw.nl/schemas/dcx/dai/"
      xmlns:dcx-gml="http://easy.dans.knaw.nl/schemas/dcx/gml/"
      xmlns:gml="http://www.opengis.net/gml"
      xmlns:abr="http://www.den.nl/standaard/166/Archeologisch-Basisregister/"
      xmlns:ddm={schemaNameSpace}
      xmlns:id-type="http://easy.dans.knaw.nl/schemas/vocab/identifier-type/"
      xsi:schemaLocation={s"$schemaNameSpace $schemaLocation"}
    >
      <ddm:profile>
        { dm.titles.withNonEmpty.map(str => <dc:title xml:lang={ lang }>{ str }</dc:title>) }
        { dm.descriptions.withNonEmpty.map(str => <dcterms:description xml:lang={ lang }>{ str }</dcterms:description>) }
        { dm.instructionsForReuse.withNonEmpty.map(str => <ddm:description descriptionType="TechnicalInfo">{ str }</ddm:description>) }
        { dm.creators.withNonEmpty.map(author => <dcx-dai:creatorDetails>{ complexContent(author, lang) }</dcx-dai:creatorDetails>) }
        { dm.datesCreated.flatMap(_.value).withNonEmpty.map(str => <ddm:created>{ str }</ddm:created>) }
        { dm.datesAvailable.flatMap(_.value).withNonEmpty.map(str => <ddm:available>{ str }</ddm:available>) }
        { dm.audiences.toSeq.flatten.map(_.key).withNonEmpty.map(key => <ddm:audience>{ key }</ddm:audience>) }
        { dm.accessRights.toSeq.map(src => <ddm:accessRights>{ src.toString }</ddm:accessRights>) }
      </ddm:profile>
      <ddm:dcmiMetadata>
        { dm.identifiers.withNonEmpty.map(id => <dcterms:identifier xsi:type={ id.schemeOrNull }>{ id.valueOrThrow }</dcterms:identifier>) }
        { dm.alternativeTitles.withNonEmpty.map(str => <dcterms:alternative xml:lang={ lang }>{ str }</dcterms:alternative>) }
        { dm.allRelations.withNonEmpty.map(basicContent(_, lang)) }
        { dm.contributors.withNonEmpty.map(author => <dcx-dai:contributorDetails>{ complexContent(author, lang) }</dcx-dai:contributorDetails>) }
        { dm.authors.map(_.rightsHolder).withNonEmpty.map(str => <dcterms:rightsHolder>{ str }</dcterms:rightsHolder>) }
        { dm.publishers.withNonEmpty.map(str => <dcterms:publisher xml:lang={ lang }>{ str }</dcterms:publisher>) }
        { dm.sources.withNonEmpty.map(str => <dc:source xml:lang={ lang }>{ str }</dc:source>) }
        { dm.types.withNonEmpty.map(src => <dcterms:type xsi:type={ src.schemeOrNull }>{ src.valueOrThrow }</dcterms:type>) }
        { dm.formats.withNonEmpty.map(src => <dcterms:format xsi:type={ src.schemeOrNull }>{ src.valueOrThrow }</dcterms:format>) }
        { dm.subjects.withNonEmpty.map(basicContent(_, "subject", lang)) }
        { dm.temporalCoverages.withNonEmpty.map(basicContent(_, "temporal", lang)) }
        { dm.spatialCoverages.withNonEmpty.map(basicContent(_, "spatial", lang)) }
        { dm.otherDates.withNonEmpty.map(date => <label xsi:type={ date.schemeOrNull }>{ date.valueOrThrow }</label>.withLabel(date)) }
        { dm.spatialPoints.withNonEmpty.map(point => <dcx-gml:spatial srsName={ point.srsName }>{ complexContent(point) }</dcx-gml:spatial>) }
        { dm.spatialBoxes.withNonEmpty.map(point => <dcx-gml:spatial>{ complexContent(point) }</dcx-gml:spatial>) }
        { dm.license.withNonEmpty.map(src => <dcterms:license xsi:type={ src.schemeOrNull }>{ src.valueOrThrow }</dcterms:license>) }
        { dm.languagesOfFiles.withNonEmpty.flatMap(src => <dcterms:language xsi:type ={ src.schemeOrNull  }>{ src.keyOrValue }</dcterms:language>) }
      </ddm:dcmiMetadata>
    </ddm:DDM>
  }.recoverWith {
    case e: IllegalArgumentException => Failure(InvalidDocumentException("DatasetMetadata", e))
  }

  private def complexContent(point: SpatialPoint): Elem = {
    <Point xmlns="http://www.opengis.net/gml">
        <pos>{ point.pos }</pos>
    </Point>
  }

  private def complexContent(box: SpatialBox): Elem = {
    <boundedBy xmlns="http://www.opengis.net/gml">
        <Envelope srsName={ box.srsName }>
            <lowerCorner>{ box.lower }</lowerCorner>
            <upperCorner>{ box.upper }</upperCorner>
        </Envelope>
    </boundedBy>
  }

  private def basicContent(relation: RelationType, lang: String): Elem = {
    (relation match {
      case Relation(_, Some(url: String), None) =>
        <label href={ relation.urlOrNull }>{ url }</label>
      case _: Relation => // Relation(_,None,None) is skipped so we do have a title (via value) and therefore a language
        <label xml:lang={ lang } href={ relation.urlOrNull }>{ relation.valueOrThrow }</label>
      case rel: RelatedIdentifier if rel.url.isEmpty =>
        <label xsi:type={ rel.schemeOrNull }>{ relation.valueOrThrow }</label>
      case rel: RelatedIdentifier =>
        <label scheme={ rel.schemeOrNull } href={ relation.urlOrNull }>{ relation.valueOrThrow }</label>
    }).withLabel(relation)
  }

  private def basicContent(source: SchemedKeyValue, label: String, lang: String): Elem = {
    (label, source) match {
      case ("subject", SchemedKeyValue(None, None, Some(value))) =>
        <label xml:lang={ lang }>{ value }</label>.withLabel(s"dc:$label")
      case (_, SchemedKeyValue(None, None, Some(value))) =>
        <label xml:lang={ lang }>{ value }</label>.withLabel(s"dcterms:$label")
      case (_, SchemedKeyValue(Some(scheme), Some(key), _)) if source.schemeNeedsKey =>
        <label xsi:type={ scheme }>{ key }</label>.withLabel(s"dcterms:$label")
      case (_, SchemedKeyValue(_, _, Some(value))) =>
        <label xml:lang={ lang } schemeURI={ source.schemeOrNull } valueURI={ source.keyOrNull }>{ value }</label>.withLabel(s"ddm:$label")
    }
  }

  private def complexContent(author: Author, lang: String): Seq[Node] = {
    if (author.surname.forall(_.isBlank))
      author.organization.toSeq.map(complexContent(_, lang, author.role))
    else
      <dcx-dai:author>
        { author.titles.withNonEmpty.map(str => <dcx-dai:titles xml:lang={ lang }>{ str }</dcx-dai:titles>) }
        { author.initials.withNonEmpty.map(str => <dcx-dai:initials>{ str }</dcx-dai:initials>) }
        { author.insertions.withNonEmpty.map(str => <dcx-dai:insertions>{ str }</dcx-dai:insertions>) }
        { author.surname.withNonEmpty.map(str => <dcx-dai:surname>{ str }</dcx-dai:surname>) }
        { author.ids.withNonEmpty.map(src => <label>{ src.valueOrThrow }</label>.withLabel(s"dcx-dai:${ src.schemeOrEmpty.replace("id-type:", "") }")) }
        { author.role.flatMap(_.key).withNonEmpty.map(key => <dcx-dai:role>{ key }</dcx-dai:role>) }
        { author.organization.withNonEmpty.map(complexContent(_, lang, role = None)) }
      </dcx-dai:author>
  }

  private def complexContent(organization: String, lang: String, role: Option[SchemedKeyValue]): Elem =
      <dcx-dai:organization>
        { <dcx-dai:name xml:lang={ lang }>{ organization }</dcx-dai:name> }
        { role.flatMap(_.key).withNonEmpty.map(key => <dcx-dai:role>{ key }</dcx-dai:role>) }
      </dcx-dai:organization>

  /** @param elem XML element to be adjusted */
  private implicit class RichElem(val elem: Elem) extends AnyVal {
    /** @param str the desired label, optionally with name space prefix */
    @throws[InvalidDocumentException]("when str is not a valid XML label (has more than one ':')")
    def withLabel(str: String): Elem = {
      str.split(":") match {
        case Array(label) if label.nonEmpty => elem.copy(label = label)
        case Array(prefix, label) => elem.copy(prefix = prefix, label = label)
        case a => throw new IllegalArgumentException(s"expecting (label) or (prefix:label); got [${ a.mkString(":") }] $msg")
      }
    }

    @throws[IllegalArgumentException]("when the relation has no qualifier")
    def withLabel(relation: RelationType): Elem = {
      val qualifier: String = relation.qualifier
        .map(_.toString)
        .getOrElse(throwMissingQualifier(JsonUtil.toJson(relation)))

      withLabel(
        if (relation.url.isEmpty) qualifier
        else qualifier.replace("dcterms", "ddm")
      )
    }

    @throws[IllegalArgumentException]("when the date has no qualifier")
    def withLabel(date: Date): Elem = {
      val qualifier: String = date.qualifier
        .map(_.toString)
        .getOrElse(throwMissingQualifier(JsonUtil.toJson(date)))

      withLabel(qualifier)
    }

    def msg = s"to adjust the <${ elem.label }> of $elem"

    private def throwMissingQualifier(json: => String) = {
      throw new IllegalArgumentException(s"no qualifier $json $msg")
    }
  }
}
