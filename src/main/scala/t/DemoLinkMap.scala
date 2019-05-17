package t

import org.onosproject.net.Link
import org.onosproject.net.LinkKey
import org.onosproject.ui.topo.BiLinkMap
import t.DemoLink


/**
  * Our concrete link map.
  */
class DemoLinkMap extends BiLinkMap[DemoLink] {
    override protected def create(linkKey: LinkKey, link: Link) = new DemoLink(linkKey, link)
}
