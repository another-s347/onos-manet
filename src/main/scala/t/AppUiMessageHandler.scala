package t

import com.fasterxml.jackson.databind.node.ObjectNode
import com.google.common.collect.ImmutableSet
import org.onosproject.ui.RequestHandler
import org.onosproject.ui.UiMessageHandler
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util


/**
  * Skeletal ONOS UI Custom-View message handler.
  */
object AppUiMessageHandler {
    private val SAMPLE_CUSTOM_DATA_REQ = "sampleCustomDataRequest"
    private val SAMPLE_CUSTOM_DATA_RESP = "sampleCustomDataResponse"
    private val NUMBER = "number"
    private val SQUARE = "square"
    private val CUBE = "cube"
    private val MESSAGE = "message"
    private val MSG_FORMAT = "Next incrememt is %d units"
}

class AppUiMessageHandler extends UiMessageHandler {
    final private val log = LoggerFactory.getLogger(getClass)
    private var someNumber = 1
    private var someIncrement = 1

    override protected def createRequestHandlers: util.Collection[RequestHandler] = ImmutableSet.of(new SampleCustomDataRequestHandler)

    // handler for sample data requests
    final class SampleCustomDataRequestHandler extends RequestHandler(AppUiMessageHandler.SAMPLE_CUSTOM_DATA_REQ) {
        override def process(payload: ObjectNode): Unit = {
            someIncrement += 1
            someNumber += someIncrement
            log.debug("Computing data for {}...", someNumber)
            val result = objectNode
            result.put(AppUiMessageHandler.NUMBER, someNumber)
            result.put(AppUiMessageHandler.SQUARE, someNumber * someNumber)
            result.put(AppUiMessageHandler.CUBE, someNumber * someNumber * someNumber)
            result.put(AppUiMessageHandler.MESSAGE, f"Next incrememt is ${someIncrement + 1} units")
            sendMessage(AppUiMessageHandler.SAMPLE_CUSTOM_DATA_RESP, result)
        }
    }

}