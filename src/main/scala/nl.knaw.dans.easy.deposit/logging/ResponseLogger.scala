package nl.knaw.dans.easy.deposit.logging

import javax.servlet.http.{ HttpServletRequest, HttpServletResponse }
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.scalatra.ActionResult

trait ResponseLogger extends DebugEnhancedLogging with ResponseLogFormatter {

  override def logResponse(actionResult: ActionResult)
                          (implicit request: HttpServletRequest,
                           response: HttpServletResponse): Unit = {
    logger.info(formatResponseLog(actionResult))
  }
}

object ResponseLogger {

  implicit class RichActionResult(val actionResult: ActionResult) extends AnyVal {
    def logResponse(implicit request: HttpServletRequest,
                    response: HttpServletResponse,
                    responseLogger: ResponseLogFormatter): ActionResult = {
      responseLogger.logResponse(actionResult)
      actionResult
    }
  }
}
