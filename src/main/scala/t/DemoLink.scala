package t

import org.onosproject.net.Link
import org.onosproject.net.LinkKey
import org.onosproject.ui.topo.BiLink
import org.onosproject.ui.topo.LinkHighlight
import org.onosproject.ui.topo.LinkHighlight.Flavor


/**
  * Our demo concrete class of a bi-link. We give it state so we can decide
  * how to create link highlights.
  */
class DemoLink(override val key: LinkKey, val link: Link) extends BiLink(key, link) {
    private var important = false
    private var label:String = null

    def makeImportant: DemoLink = {
        important = true
        this
    }

    def setLabel(label: String): DemoLink = {
        this.label = label
        this
    }

    override def highlight(anEnum: Enum[_]): LinkHighlight = {
        val flavor = if (important) Flavor.PRIMARY_HIGHLIGHT
        else Flavor.SECONDARY_HIGHLIGHT
        new LinkHighlight(this.linkId, flavor).setLabel(label)
    }

}
