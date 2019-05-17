package t

import com.fasterxml.jackson.databind.node.ObjectNode
import com.google.common.base.Strings
import com.google.common.collect.ImmutableSet
import org.onlab.osgi.ServiceDirectory
import org.onosproject.net._
import org.onosproject.net.device.DeviceService
import org.onosproject.net.host.HostService
import org.onosproject.net.link.LinkService
import org.onosproject.ui.RequestHandler
import org.onosproject.ui.UiConnection
import org.onosproject.ui.UiMessageHandler
import org.onosproject.ui.topo.DeviceHighlight
import org.onosproject.ui.topo.Highlights
import org.onosproject.ui.topo.NodeBadge
import org.onosproject.ui.topo.NodeBadge.Status
import org.onosproject.ui.topo.TopoJson
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util
import java.util.Timer
import java.util.TimerTask

import t.AppUiTopovMessageHandler.Mode


/**
  * Skeletal ONOS UI Topology-Overlay message handler.
  */
object AppUiTopovMessageHandler {
    private val SAMPLE_TOPOV_DISPLAY_START = "sampleTopovDisplayStart"
    private val SAMPLE_TOPOV_DISPLAY_UPDATE = "sampleTopovDisplayUpdate"
    private val SAMPLE_TOPOV_DISPLAY_STOP = "sampleTopovDisplayStop"
    private val ID = "id"
    private val MODE = "mode"
    private val UPDATE_PERIOD_MS = 1000
    private val EMPTY_LINK_SET = Array.empty[Link]

    private object Mode extends Enumeration {
        type Mode = Value
        val IDLE, MOUSE, LINK = Value
    }

}

class AppUiTopovMessageHandler extends UiMessageHandler {
    final private val log = LoggerFactory.getLogger(getClass)
    private var deviceService:DeviceService = null
    private var hostService:HostService = null
    private var linkService:LinkService = null
    final private val timer = new Timer("sample-overlay")
    private var demoTask:TimerTask = null
    private var currentMode = AppUiTopovMessageHandler.Mode.IDLE
    private var elementOfNote:Element = null
    private var linkSet = AppUiTopovMessageHandler.EMPTY_LINK_SET
    private var linkIndex = 0

    override def init(connection: UiConnection, directory: ServiceDirectory): Unit = {
        super.init(connection, directory)
        deviceService = directory.get(classOf[DeviceService])
        hostService = directory.get(classOf[HostService])
        linkService = directory.get(classOf[LinkService])
    }

    override protected def createRequestHandlers: util.Collection[RequestHandler] = ImmutableSet.of(new DisplayStartHandler, new DisplayUpdateHandler, new DisplayStopHandler)

    final private class DisplayStartHandler() extends RequestHandler(AppUiTopovMessageHandler.SAMPLE_TOPOV_DISPLAY_START) {
        override def process(payload: ObjectNode): Unit = {
            val mode = string(payload, AppUiTopovMessageHandler.MODE)
            log.info("Start Display: mode [{}]", mode)
            clearState()
            clearForMode()
            mode match {
                case "mouse" =>
                    currentMode = AppUiTopovMessageHandler.Mode.MOUSE
                    cancelTask()
                    sendMouseData()
                case "link" =>
                    currentMode = AppUiTopovMessageHandler.Mode.LINK
                    scheduleTask()
                    initLinkSet()
                    sendLinkData()
                case _ =>
                    currentMode = AppUiTopovMessageHandler.Mode.IDLE
                    cancelTask()
            }
        }
    }

    final private class DisplayUpdateHandler() extends RequestHandler(AppUiTopovMessageHandler.SAMPLE_TOPOV_DISPLAY_UPDATE) {
        override def process(payload: ObjectNode): Unit = {
            val id = string(payload, AppUiTopovMessageHandler.ID)
            log.debug("Update Display: id [{}]", id)
            if (!Strings.isNullOrEmpty(id)) updateForMode(id)
            else clearForMode()
        }
    }

    final private class DisplayStopHandler() extends RequestHandler(AppUiTopovMessageHandler.SAMPLE_TOPOV_DISPLAY_STOP) {
        override def process(payload: ObjectNode): Unit = {
            log.debug("Stop Display")
            cancelTask()
            clearState()
            clearForMode()
        }
    }

    private def clearState(): Unit = {
        currentMode = AppUiTopovMessageHandler.Mode.IDLE
        elementOfNote = null
        linkSet = AppUiTopovMessageHandler.EMPTY_LINK_SET
    }

    private def updateForMode(id: String): Unit = {
        log.debug("host service: {}", hostService)
        log.debug("device service: {}", deviceService)
        try {
            val hid = HostId.hostId(id)
            log.debug("host id {}", hid)
            elementOfNote = hostService.getHost(hid)
            log.debug("host element {}", elementOfNote)
        } catch {
            case e: Exception =>
                try {
                    val did = DeviceId.deviceId(id)
                    log.debug("device id {}", did)
                    elementOfNote = deviceService.getDevice(did)
                    log.debug("device element {}", elementOfNote)
                } catch {
                    case e2: Exception =>
                        log.debug("Unable to process ID [{}]", id)
                        elementOfNote = null
                }
        }
        currentMode match {
            case Mode.MOUSE =>
                sendMouseData()
            case Mode.LINK =>
                sendLinkData()
            case _ =>
        }
    }

    private def clearForMode(): Unit = {
        sendHighlights(new Highlights)
    }

    private def sendHighlights(highlights: Highlights): Unit = {
        sendMessage(TopoJson.highlightsMessage(highlights))
    }

    private def sendMouseData(): Unit = {
        if (elementOfNote != null && elementOfNote.isInstanceOf[Device]) {
            val devId = elementOfNote.id.asInstanceOf[DeviceId]
            val links = linkService.getDeviceEgressLinks(devId)
            val highlights = fromLinks(links, devId)
            addDeviceBadge(highlights, devId, links.size)
            sendHighlights(highlights)
        }
        // Note: could also process Host, if available
    }

    private def addDeviceBadge(h: Highlights, devId: DeviceId, n: Int): Unit = {
        val dh = new DeviceHighlight(devId.toString)
        dh.setBadge(createBadge(n))
        h.add(dh)
    }

    private def createBadge(n: Int) = {
        val status = if (n > 3) Status.ERROR
        else Status.WARN
        val noun = if (n > 3) "(critical)"
        else "(problematic)"
        val msg = "Egress links: " + n + " " + noun
        NodeBadge.number(status, n, msg)
    }

    private def fromLinks(links: util.Set[Link], devId: DeviceId) = {
        val linkMap = new DemoLinkMap
        if (links != null) {
            log.debug("Processing {} links", links.size)
            links.forEach(linkMap.add)
        }
        else log.debug("No egress links found for device {}", devId)
        val highlights = new Highlights
        import scala.collection.JavaConversions._
        for (dlink <- linkMap.biLinks) {
            dlink.makeImportant.setLabel("Yo!")
            highlights.add(dlink.highlight(null))
        }
        highlights
    }

    private def initLinkSet(): Unit = {
        val links:scala.collection.mutable.HashSet[Link] = scala.collection.mutable.HashSet.empty
        linkService.getActiveLinks.forEach(link=>{
            links.add(link)
        })
        linkSet = links.toArray
        linkIndex = 0
        log.debug("initialized link set to {}", linkSet.length)
    }

    private def sendLinkData(): Unit = {
        val linkMap = new DemoLinkMap
        for (link <- linkSet) {
            linkMap.add(link)
        }
        val dl = linkMap.add(linkSet(linkIndex))
        dl.makeImportant.setLabel(Integer.toString(linkIndex))
        log.debug("sending link data (index {})", linkIndex)
        linkIndex += 1
        if (linkIndex >= linkSet.length) linkIndex = 0
        val highlights = new Highlights
        linkMap.biLinks().forEach(dlink=>{
            highlights.add(dlink.highlight(null))
        })
        sendHighlights(highlights)
    }

    private def scheduleTask(): Unit = {
        if (demoTask == null) {
            log.debug("Starting up demo task...")
            demoTask = new DisplayUpdateTask
            timer.schedule(demoTask, AppUiTopovMessageHandler.UPDATE_PERIOD_MS, AppUiTopovMessageHandler.UPDATE_PERIOD_MS)
        }
        else log.debug("(demo task already running")
    }

    private def cancelTask(): Unit = {
        if (demoTask != null) {
            demoTask.cancel
            demoTask = null
        }
    }

    private class DisplayUpdateTask extends TimerTask {
        override def run(): Unit = {
            try
                currentMode match {
                    case Mode.LINK =>
                        sendLinkData()
                    case _ =>
                }
            catch {
                case e: Exception =>
                    log.warn("Unable to process demo task: {}", e.getMessage)
                    log.debug("Oops", e)
            }
        }
    }

}
