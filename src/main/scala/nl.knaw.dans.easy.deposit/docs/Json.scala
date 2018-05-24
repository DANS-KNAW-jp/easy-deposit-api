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

import java.nio.file.{ Path, Paths }

import nl.knaw.dans.easy.deposit.docs.DatasetMetadata.{ DateQualifier, _ }
import nl.knaw.dans.easy.deposit.docs.StateInfo.State
import org.json4s.Extraction.decompose
import org.json4s.JsonAST._
import org.json4s.ext.{ EnumNameSerializer, JodaTimeSerializers, UUIDSerializer }
import org.json4s.native.JsonMethods
import org.json4s.native.Serialization.write
import org.json4s.{ CustomSerializer, DefaultFormats, Diff, Extraction, Formats, JValue, JsonInput }

import scala.reflect.runtime.universe.typeOf
import scala.util.{ Failure, Success, Try }

object Json {

  case class InvalidDocumentException(s: String, t: Throwable = null)
    extends Exception(s"invalid $s: ${ t.getClass } ${ t.getMessage }", t)

  class PathSerializer extends CustomSerializer[Path](_ =>
    ( {
      case JString(s) => Paths.get(s)
      case JNull => null
    }, {
      case x: Path => JString(x.toString)
    }
    )
  )

  // NB: values for all enums should be unique, see https://github.com/json4s/json4s/issues/142
  private val enumerations = List(DateQualifier, State, AccessCategory, PrivacySensitiveDataPresent)

  private implicit val jsonFormats: Formats = new DefaultFormats {} +
    UUIDSerializer +
    new PathSerializer ++
    enumerations.map(new EnumNameSerializer(_)) ++
    JodaTimeSerializers.all

  implicit class RichJsonInput(body: JsonInput) {
    def deserialize[A: Manifest]: Try[A] = {
      for {
        parsed <- Try { JsonMethods.parse(body) }
        _ <- acceptOnlyJObject(parsed)
        result = Extraction.extract(parsed)
        _ <- rejectNotExpectedContent(parsed, result)
      } yield result
    }.recoverWith { case t: Throwable =>
      val className = typeOf[A].typeSymbol.name.toString
      Failure(InvalidDocumentException(className, t))
    }

    private def rejectNotExpectedContent[T](parsed: JValue, extracted: T): Try[Unit] = {
      decompose(extracted) diff parsed match {
        case Diff(_, JNothing, _) => Success(())
        case Diff(_, ignored, _) => Failure(new Exception(s"don't recognize ${ write(ignored) }"))
      }
    }

    private def acceptOnlyJObject(parsed: JValue): Try[Unit] = {
      if (parsed.isInstanceOf[JObject]) Success(())
      else Failure(new Exception(s"expected an object, got a ${ parsed.getClass }"))
    }
  }

  def toJson[A <: AnyRef](a: A): String = {
    // seems not to need a try: while the date formatter wasn't in place it produced empty strings
    write(a)
  }

  def getStateInfo(body: JsonInput): Try[StateInfo] = body.deserialize[StateInfo]
}
