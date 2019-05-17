package t

import org.onosproject.net.DeviceId
import org.onosproject.ui.UiTopoOverlay
import org.onosproject.ui.topo.ButtonId
import org.onosproject.ui.topo.PropertyPanel
import org.onosproject.ui.topo.TopoConstants.CoreButtons
import org.onosproject.ui.GlyphConstants
import org.onosproject.ui.topo.TopoConstants.Properties.FLOWS
import org.onosproject.ui.topo.TopoConstants.Properties.INTENTS
import org.onosproject.ui.topo.TopoConstants.Properties.LATITUDE
import org.onosproject.ui.topo.TopoConstants.Properties.LONGITUDE
import org.onosproject.ui.topo.TopoConstants.Properties.TOPOLOGY_SSCS
import org.onosproject.ui.topo.TopoConstants.Properties.TUNNELS
import org.onosproject.ui.topo.TopoConstants.Properties.VERSION


/**
  * Our topology overlay.
  */
object AppUiTopovOverlay { // NOTE: this must match the ID defined in sampleTopov.js
    private val OVERLAY_ID = "meowster-overlay"
    private val MY_TITLE = "My App Rocks!"
    private val MY_VERSION = "alpha-0.0.1"
    private val MY_DEVICE_TITLE = "I changed the title"
    private val FOO_BUTTON = new ButtonId("foo")
    private val BAR_BUTTON = new ButtonId("bar")
}

class AppUiTopovOverlay() extends UiTopoOverlay(AppUiTopovOverlay.OVERLAY_ID) {
    override def modifySummary(pp: PropertyPanel): Unit = {
        pp.title(AppUiTopovOverlay.MY_TITLE)
          .glyphId(GlyphConstants.CROWN)
          .removeProps(TOPOLOGY_SSCS, INTENTS, TUNNELS, FLOWS, VERSION)
//          .addProp(VERSION, AppUiTopovOverlay.MY_VERSION)
    }

    override def modifyDeviceDetails(pp: PropertyPanel, deviceId: DeviceId): Unit = {
        pp.title(AppUiTopovOverlay.MY_DEVICE_TITLE)
        pp.removeProps(LATITUDE, LONGITUDE)
        pp.addButton(AppUiTopovOverlay.FOO_BUTTON).addButton(AppUiTopovOverlay.BAR_BUTTON)
        pp.removeButtons(CoreButtons.SHOW_PORT_VIEW).removeButtons(CoreButtons.SHOW_GROUP_VIEW).removeButtons(CoreButtons.SHOW_METER_VIEW)
    }
}
