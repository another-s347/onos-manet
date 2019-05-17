package t

import com.google.common.collect.ImmutableList
import org.osgi.service.component.annotations.Activate
import org.osgi.service.component.annotations.Component
import org.osgi.service.component.annotations.Deactivate
import org.osgi.service.component.annotations.Reference
import org.osgi.service.component.annotations.ReferenceCardinality
import org.onosproject.ui.UiExtension
import org.onosproject.ui.UiExtensionService
import org.onosproject.ui.UiMessageHandlerFactory
import org.onosproject.ui.UiTopoOverlayFactory
import org.onosproject.ui.UiView
import org.onosproject.ui.UiViewHidden
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import t.AppUiTopovMessageHandler
import t.AppUiTopovOverlay
import java.util
import scala.collection.JavaConverters._


/**
  * Skeletal ONOS UI Topology-Overlay application component.
  */
@Component(immediate = true) object AppUiTopovComponent {
    private val CL = classOf[AppUiTopovComponent].getClassLoader
    private val VIEW_ID = "sampleTopov"
}

@Component(immediate = true) class AppUiTopovComponent {
    final private val log = LoggerFactory.getLogger(getClass)
    @Reference(cardinality = ReferenceCardinality.MANDATORY) protected var uiExtensionService: UiExtensionService = null
    // List of application views
    final private val uiViews:List[UiView] = List(new UiViewHidden(AppUiTopovComponent.VIEW_ID))
    // Factory for UI message handlers
    final private val messageHandlerFactory:UiMessageHandlerFactory = () => ImmutableList.of(new AppUiTopovMessageHandler)
    // Factory for UI topology overlays
    final private val topoOverlayFactory:UiTopoOverlayFactory = () => ImmutableList.of(new AppUiTopovOverlay)
    // Application UI extension
    protected var extension: UiExtension = new UiExtension.Builder(AppUiTopovComponent.CL, uiViews.asJava).resourcePath(AppUiTopovComponent.VIEW_ID).messageHandlerFactory(messageHandlerFactory).topoOverlayFactory(topoOverlayFactory).build

    @Activate protected def activate(): Unit = {
        uiExtensionService.register(extension)
        log.info("Started")
    }

    @Deactivate protected def deactivate(): Unit = {
        uiExtensionService.unregister(extension)
        log.info("Stopped")
    }
}
