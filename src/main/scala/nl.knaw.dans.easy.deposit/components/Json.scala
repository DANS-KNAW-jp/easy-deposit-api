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
package nl.knaw.dans.easy.deposit.components

import java.nio.file.{ Path, Paths }
import java.text.SimpleDateFormat

import nl.knaw.dans.easy.deposit.{ DatasetMetadata, State, StateInfo }
import org.json4s
import org.json4s.JsonAST._
import org.json4s.ext.{ EnumNameSerializer, JodaTimeSerializers, UUIDSerializer }
import org.json4s.native.JsonMethods
import org.json4s.native.Serialization.write
import org.json4s.{ CustomSerializer, DefaultFormats, Formats, JsonInput }

import scala.util.{ Failure, Try }

object Json {

  // TODO rename to InvalidDocumentException (would cause merge conflicts with PR #20)
  case class InvalidDocument(s: String, t: Throwable) extends Exception(s, t)

  class PathSerializer extends CustomSerializer[Path](format =>
    ( {
      case JString(s) => Paths.get(s)
      case JNull => null
    }, {
      case x: Path => JString(x.toString)
    }
    )
  )

  private implicit val jsonFormats: Formats = new DefaultFormats {
    // we need a timestamp for DepositInfo, dates in DatasetMetadata are plain strings so no conflict
    override protected def dateFormatter: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
  } +
    UUIDSerializer +
    new PathSerializer +
    new EnumNameSerializer(State) ++
    JodaTimeSerializers.all

  def toJson[A <: AnyRef](a: A): String = {
    // seems not to need a try: while the date formatter wasn't in place it produced empty strings
    write(a)
  }

  def getUser(body: JsonInput): Try[UserInfo] = {
    parseObject(body).map(_.extract[UserInfo])
  }.recoverWith { case t: Throwable => Failure(InvalidDocument("User", t)) }

  def getStateInfo(body: JsonInput): Try[StateInfo] = {
    parseObject(body).map(_.extract[StateInfo])
  }.recoverWith { case t: Throwable => Failure(InvalidDocument("StateInfo", t)) }

  def getDatasetMetadata(body: JsonInput): Try[DatasetMetadata] = {
    parseObject(body).map(_.extract[DatasetMetadata])
  }.recoverWith { case t: Throwable => Failure(InvalidDocument("DatasetMetadata", t)) }

  private def parseObject(body: JsonInput): Try[json4s.JValue] = Try {
    JsonMethods.parse(body)
  }.map {
    case jObject if jObject.isInstanceOf[JObject] => jObject
    case jValue => throw new Exception(s"expected an object, got a ${ jValue.getClass }")
  }
}
