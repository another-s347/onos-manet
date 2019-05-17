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
import org.onosproject.ui.UiView
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util


/**
  * Skeletal ONOS UI Custom-View application component.
  */
@Component(immediate = true) object AppUiComponent {
    private val VIEW_ID = "sampleCustom"
    private val VIEW_TEXT = "Sample Custom"
}

@Component(immediate = true) class AppUiComponent {
    final private val log = LoggerFactory.getLogger(getClass)
    @Reference(cardinality = ReferenceCardinality.MANDATORY) protected var uiExtensionService: UiExtensionService = null
    // List of application views
    final private val uiViews = ImmutableList.of(new UiView(UiView.Category.OTHER, AppUiComponent.VIEW_ID, AppUiComponent.VIEW_TEXT))
    // Factory for UI message handlers
    final private val messageHandlerFactory:UiMessageHandlerFactory = () => ImmutableList.of(new AppUiMessageHandler)
    // Application UI extension
    protected var extension: UiExtension = new UiExtension.Builder(getClass.getClassLoader, uiViews).resourcePath(AppUiComponent.VIEW_ID).messageHandlerFactory(messageHandlerFactory).build

    @Activate protected def activate(): Unit = {
        uiExtensionService.register(extension)
        log.info("Started")
    }

    @Deactivate protected def deactivate(): Unit = {
        uiExtensionService.unregister(extension)
        log.info("Stopped")
    }
}
