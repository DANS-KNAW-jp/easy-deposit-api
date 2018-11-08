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
package nl.knaw.dans.easy.deposit.logging

import javax.servlet.http.{ HttpServletRequest, HttpServletResponse }
import org.scalatra.ActionResult
import scala.collection.JavaConverters._

trait ResponseLogFormatter extends CookieFormatter {

  /** Assembles the content for a log line.
   *
   * Adding the logResponse method to [[ActionResult]] for the following examples is explained at package level.
   *
   * @example
   * {{{
   *   // usage in servlets
   *
   *   ... extends ScalatraServlet {
   *     get(???) {
   *       ???
   *       Ok(body = "hello world").logResponse
   *     }
   *   }
   * }}}
   *
   * An after method would not be executed at all after a halt.
   * @example
   * {{{
   *   // usage in a trait for servlets that extends ScentrySupport.
   *
   *   halt(Unauthorized(???).logResponse)
   * }}}
   * @param actionResult the response created by the servlet
   * @param request      some info is used as a kind of bracketing the response log line with the request log line
   * @param response     here we might find some info created by a [[org.scalatra.auth.ScentrySupport]] trait
   *                     for a [[org.scalatra.ScalatraServlet]].
   * @return
   */
  def formatResponseLog(actionResult: ActionResult)
                       (implicit request: HttpServletRequest, response: HttpServletResponse): String = {
    s"${ request.getMethod } returned status=${ actionResult.status }; authHeaders=${ authHeadersToString }; actionHeaders=${ actionHeadersToString(actionResult) }"
  }

  protected def actionHeadersToString(actionResult: ActionResult): String =
    formatActionHeaders(actionResult).mkString("[", ",", "]")

  protected def formatActionHeaders(actionResult: ActionResult): Map[String, String] =
    actionResult.headers // TODO multiple values for one header

  protected def authHeadersToString(implicit response: HttpServletResponse): String =
    formatAuthHeaders(response).map(kv => kv._1 -> kv._2.mkString("[", ", ", "]")).mkString("[", ", ", "]")

  /** Formats the values of headers with (case insensitive) name REMOTE_USER and Set-Cookie */
  protected def formatAuthHeaders(implicit response: HttpServletResponse): Map[String, Iterable[String]] = {
    response.getHeaderNames.toArray().map {
      case name: String if "set-cookie" == name.toLowerCase => formatHeaderValues(name, formatCookieValue)
      case name: String if "remote_user" == name.toLowerCase => formatHeaderValues(name, formatRemoteUserValue)
      case name: String => name -> response.getHeaders(name).asScala
    }.toMap
  }

  private def formatHeaderValues(name: String, formatter: String => String)(implicit response: HttpServletResponse) = {
    val formattedValues = response.getHeaders(name)
      .asScala
      .map(formatter)
    (name, formattedValues)
  }

  protected def formatRemoteUserValue(value: String): String = "*****"
}
