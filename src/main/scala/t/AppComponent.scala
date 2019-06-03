/*
 * Copyright 2019-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package t

import java.nio.ByteBuffer
import java.util
import java.util.{Optional, Timer, TimerTask}

import org.apache.felix.scr.annotations.{Component, Reference, ReferenceCardinality}
import org.onlab.graph.{ScalarWeight, Weight}
import org.onlab.packet.{UDP, _}
import org.onlab.util.DataRateUnit
import org.onosproject.cfg.ComponentConfigService
import org.onosproject.core.{ApplicationId, CoreService}
import org.onosproject.net._
import org.onosproject.net.device.DeviceService
import org.onosproject.net.flow._
import org.onosproject.net.flowobjective.FlowObjectiveService
import org.onosproject.net.host.HostService
import org.onosproject.net.intent.constraint.BandwidthConstraint
import org.onosproject.net.intent.{Constraint, HostToHostIntent, Intent, IntentCompiler, IntentExtensionService, IntentService, IntentStore, Key, LinkCollectionIntent, PointToPointIntent}
import org.onosproject.net.link.{LinkProviderRegistry, LinkService, LinkStore}
import org.onosproject.net.packet._
import org.onosproject.net.statistic.{FlowStatisticService, StatisticService}
import org.onosproject.net.topology.{LinkWeigher, PathService, TopologyEdge, TopologyService}
import org.osgi.service.component.ComponentContext
import org.osgi.service.component.annotations.{Activate, Deactivate}
import org.slf4j.LoggerFactory
import t.Intent.{Ipv4Constraint, LinkCollectionIntentCompiler, TableIdConstraint}

import scala.collection.JavaConverters._
import scala.collection.mutable


/**
 * Skeletal ONOS application component.
 */
@Component(immediate = true)
class AppComponent {
    final private val log = LoggerFactory.getLogger(getClass)
    val devicePorcessor = new MyDeviceProcessor
    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY) var topologyService: TopologyService = _
    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY) var cfgService: ComponentConfigService = _
    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY) var coreService: CoreService = _
    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY) var flowRuleService: FlowRuleService = _
    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY) var packetService: PacketService = _
    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY) var hostService: HostService = _
    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY) var flowObjectiveService: FlowObjectiveService = _
    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY) var deviceService: DeviceService = _
    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY) var pathService: PathService = _
    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY) var statService: StatisticService = _
    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY) var flowStatService: FlowStatisticService = _
    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY) var intentExtension: IntentExtensionService = _
    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY) var intentService: IntentService = _
    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY) var linkService: LinkService = _
    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY) var linkStore: LinkStore = _
    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY) var linkProviderRegistry: LinkProviderRegistry = _
    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY) var intentStore: IntentStore = _
    var appId: ApplicationId = _
    var targetAppId: ApplicationId = _
    var packetProcessor = new MyPacketProcessor
    var devices: Set[DeviceId] = Set.empty
    var task: Option[PortStatsTask] = None
    var old_compiler: Option[IntentCompiler[LinkCollectionIntent]] = None
    var intents: java.util.concurrent.ConcurrentHashMap[Key, MonitorStatus] = new java.util.concurrent.ConcurrentHashMap()
    var linkRates: mutable.HashMap[Link, Long] = mutable.HashMap.empty
    var deviceRates: mutable.HashMap[DeviceId, Long] = mutable.HashMap.empty
    val timer = new Timer()
    val linkWeigher = new MyLinkWeigher
    val emptyWeigher = new EmptyLinkWeigher

    @Activate def activate(context: ComponentContext): Unit = {
        //cfgService.registerProperties(getClass)
        appId = coreService.registerApplication("test scala")
        targetAppId = coreService.registerApplication("test scala target")

        packetService.addProcessor(packetProcessor, PacketProcessor.director(2))
        requestIntercepts()
        intentService.getIntents.forEach(intent => {
            if (intent.appId() == appId) {
                intentService.withdraw(intent)
                intentService.purge(intent)
            }
            if (intent.appId() == targetAppId) {
                intentService.withdraw(intent)
                intentService.purge(intent)
            }
        })
        task = Some(new PortStatsTask)
        task.get.schedule()
        log.info("scala Started")
        val compilpers = intentExtension.getCompilers.asScala
        old_compiler = compilpers.get(classOf[LinkCollectionIntent]).asInstanceOf[Option[IntentCompiler[LinkCollectionIntent]]]
        intentExtension.unregisterCompiler(classOf[LinkCollectionIntent])
        intentExtension.registerCompiler(classOf[LinkCollectionIntent], new LinkCollectionIntentCompiler(hostService, appId))
    }

    def requestIntercepts(): Unit = {
        val selector = DefaultTrafficSelector.builder()
        selector.matchEthType(Ethernet.TYPE_IPV4)
        packetService.requestPackets(selector.build(), PacketPriority.REACTIVE, appId)
    }

    @Deactivate def deactivate(): Unit = {
        withdrawIntercepts()
        intents.forEach((_, m) => {
            intentService.withdraw(m.intent)
            intentService.purge(m.intent)
        })
        packetService.removeProcessor(packetProcessor)
        packetProcessor = null
        task.get.cancel()
        intentExtension.unregisterCompiler(classOf[LinkCollectionIntent])
        intentExtension.registerCompiler(classOf[LinkCollectionIntent], old_compiler.get)
        log.info("Stopped")
    }

    def withdrawIntercepts(): Unit = {
        val selector = DefaultTrafficSelector.builder()
        selector.matchEthType(Ethernet.TYPE_IPV4)
        packetService.cancelPackets(selector.build(), PacketPriority.REACTIVE, appId)
    }

    private def toMacAddress(deviceId: DeviceId) = { // Example of deviceId.toString(): "of:0000000f6002ff6f"
        // The associated MAC address is "00:0f:60:02:ff:6f"
        val tmp1 = deviceId.toString.substring(7)
        val tmp2 = tmp1.substring(0, 2) + ":" + tmp1.substring(2, 4) + ":" + tmp1.substring(4, 6) + ":" + tmp1.substring(6, 8) + ":" + tmp1.substring(8, 10) + ":" + tmp1.substring(10, 12)
        //log.info("toMacAddress: deviceId = {}, Mac = {}", deviceId.toString(), tmp2);
        MacAddress.valueOf(tmp2)
    }

    class MyPacketProcessor extends PacketProcessor {
        override def process(context: PacketContext): Unit = {
            devicePorcessor.process(context.inPacket().receivedFrom().deviceId())

            if (context.isHandled) {
                return
            }

            val inPacket: InboundPacket = context.inPacket()
            val ethPacket = inPacket.parsed()

            if (ethPacket == null) return

            if (isControlPacket(ethPacket)) {
                log.info("is control packet")
                return
            }

            if (ethPacket.getEtherType == Ethernet.TYPE_IPV6 && ethPacket.isMulticast) {
                return
            }

            if (ethPacket.getDestinationMAC.isBroadcast) {
                hostService.getHosts.forEach(host=>{
                    val location = host.location()
                    packetOut(context, location)
                })
                return
//                if (topologyService.isBroadcastPoint(topologyService.currentTopology(), context.inPacket().receivedFrom())) {
//                    packetOut(context, PortNumber.FLOOD)
//                    return
//                }
            }

            if (ethPacket.getEtherType != Ethernet.TYPE_IPV4) {
                return
            }

            val ipv4pkt = ethPacket.getPayload.asInstanceOf[IPv4]

            if (ipv4pkt == null) {
                log.info("err")
                return
            }

            val hostId = HostId.hostId(ethPacket.getDestinationMAC)
            if (hostId.mac().isLldp) {
                return
            }

            val dstIp = Ip4Address.valueOf(ipv4pkt.getDestinationAddress)
            if (dstIp.isLinkLocal || dstIp.isMulticast || dstIp.isZero || dstIp == Ip4Address.valueOf("192.168.1.255") || dstIp == Ip4Address.valueOf("255.255.255.255")) {
                flood(context)
                return
            }

            if (isTargetPacket(ipv4pkt)) {
                handleTargetPacket(context, inPacket)
            }
            else {
                handleNormalPacket(dstIp, context, ipv4pkt, inPacket, ethPacket)
            }
        }

        def handleNormalPacket(dstIp: Ip4Address, context: PacketContext, ipv4pkt: IPv4, inPacket: InboundPacket, ethPacket: Ethernet): Unit = {
            val dsthosts = hostService.getHostsByIp(dstIp)
            if (dsthosts.size() != 1) {
                log.info("dstIp == {}", dstIp.toString)
                log.info("dstHosts size == {}", dsthosts.size())
                log.info("flood")
                flood(context)
                return
            }
            val dstHost = dsthosts.iterator().next()

            val srcIp = Ip4Address.valueOf(ipv4pkt.getSourceAddress)
            val hosts = hostService.getHostsByIp(srcIp)
            val srcHost = hosts.iterator().next()

            val constraints = List[Constraint](
                BandwidthConstraint of(30, DataRateUnit.MBPS),
                Ipv4Constraint.of(srcHost.id(), srcIp),
                Ipv4Constraint.of(dstHost.id(), dstIp),
                //                    new AnnotationConstraint("custom",2.00)
            ).asJava

            val path = setConnectivity(srcHost, dstHost, constraints, target = false)
            if(path.isDefined&&true) {
                val curDevice = inPacket.receivedFrom().deviceId()
                val packet = inPacket.parsed()
                findNextHop(curDevice, path.get).foreach(p=>{
                    p.dst().elementId() match {
                        case _:HostId=>
                            packet.setSourceMACAddress(srcHost.mac())
                            packet.setDestinationMACAddress(dstHost.mac())
                            val treatment = DefaultTrafficTreatment.builder()
                                    .setOutput(p.src().port())
                                    .build()
                            packetService.emit(new DefaultOutboundPacket(curDevice, treatment, ByteBuffer.wrap(packet.serialize())))
                        case deviceId:DeviceId=>
                            packet.setSourceMACAddress(toMacAddress(curDevice))
                            packet.setDestinationMACAddress(toMacAddress(deviceId))
                            val treatment = DefaultTrafficTreatment.builder()
                                .setOutput(p.src().port())
                                .build()
                            packetService.emit(new DefaultOutboundPacket(curDevice, treatment, ByteBuffer.wrap(packet.serialize())))
                    }
                })
            }
        }

        def setConnectivity(srcHost: Host, dstHost: Host, constraints: util.List[Constraint], target: Boolean): Option[Path] = {
            val id = if(target) targetAppId else appId
            val key = Key.of(f"${srcHost.id()},${dstHost.id()},$target", id)
            if (intents.contains(key)) {
                None
            }
            else {
                val weigher = if(target) linkWeigher else emptyWeigher
                val path = pathService.getPaths(srcHost.id(), dstHost.id(),weigher).iterator().next()
                if (path == null) {
                    log.info(f"no path found")
                    return None
                }
                val firstLink = path.links().get(0)
                val lastLink = path.links().get(path.links().size() - 1)
                if(path.links().size()==2) {
                    val intentBuilder = HostToHostIntent.builder()
                            .appId(id)
                            .key(key)
                            .constraints(constraints)
                            .priority(15)
                            .one(srcHost.id())
                            .two(dstHost.id())
                    val intent = intentBuilder.build()
                    intentService.submit(intent)
                    Some(path)
                }
                else {
                    val intentBuilder = PointToPointIntent.builder()
                        .filteredIngressPoint(new FilteredConnectPoint(firstLink.dst()))
                        .filteredEgressPoint(new FilteredConnectPoint(lastLink.src()))
                        .constraints(constraints)
                        .priority(15)
                        .appId(id)
                        .key(key)
                        .suggestedPath(path.links().subList(1, path.links().size() - 1))
                    val intent = intentBuilder.build()
                    intents.put(key, MonitorStatus(path, intent, src = srcHost, dst = dstHost, intentBuilder, target))
                    //intents.put(key, MonitorStatus(path, intent, src = srcHost, dst = dstHost, intentBuilder))
                    intentService.submit(intent)
                    Some(path)
                }
            }
        }

        def findNextHop(curDevice:DeviceId, path: Path): Option[Link] = {
            path.links().forEach(link=>{
                if(link.src().elementId()==curDevice) {
                    return Some(link)
                }
            })
            None
        }

        def flood(context: PacketContext): Unit = {
            if (topologyService.isBroadcastPoint(topologyService.currentTopology(), context.inPacket().receivedFrom())) {
                packetOut(context, PortNumber.FLOOD)
            }
            else {
                log.info("block at device == {}", context.inPacket().receivedFrom().deviceId())
                log.info("block at port == {}", context.inPacket().receivedFrom().port())
                context.block()
            }
        }

        def packetOut(context: PacketContext, connectPoint: ConnectPoint): Unit ={
            val device = connectPoint.deviceId()
            val treatment = DefaultTrafficTreatment.builder().setOutput(connectPoint.port()).build()
            val packet = new DefaultOutboundPacket(device,treatment,context.inPacket().unparsed())
            packetService.emit(packet)
        }

        def packetOut(context: PacketContext, number: PortNumber): Unit = {
            context.treatmentBuilder().setOutput(number)
            context.send()
        }

        def handleTargetPacket(context: PacketContext, inPacket: InboundPacket): Unit = {
            val ethPacket = inPacket.parsed()
            val ipv4pkt = ethPacket.getPayload.asInstanceOf[IPv4]
            val dstIp = Ip4Address.valueOf(ipv4pkt.getDestinationAddress)
            val dsthosts = hostService.getHostsByIp(dstIp)
            if (dsthosts.size() != 1) {
                log.info("dstIp == {}", dstIp.toString)
                log.info("dstHosts size == {}", dsthosts.size())
                log.info("drop")
                return
            }
            val dstHost = dsthosts.iterator().next()

            val srcIp = Ip4Address.valueOf(ipv4pkt.getSourceAddress)
            val hosts = hostService.getHostsByIp(srcIp)
            val srcHost = hosts.iterator().next()

            val constraints = List[Constraint](
                BandwidthConstraint of(30, DataRateUnit.MBPS),
                Ipv4Constraint.of(srcHost.id(), srcIp),
                Ipv4Constraint.of(dstHost.id(), dstIp),
                TableIdConstraint.of(3)
                //                    new AnnotationConstraint("custom",2.00)
            ).asJava

            val path=setConnectivity(srcHost, dstHost, constraints, target = true)
            if(path.isDefined&&true) {
                val curDevice = inPacket.receivedFrom().deviceId()
                val packet = inPacket.parsed()
                findNextHop(curDevice, path.get).foreach(p=>{
                    p.dst().elementId() match {
                        case _:HostId=>
                            packet.setSourceMACAddress(srcHost.mac())
                            packet.setDestinationMACAddress(dstHost.mac())
                            val treatment = DefaultTrafficTreatment.builder()
                                .setOutput(p.src().port())
                                .build()
                            packetService.emit(new DefaultOutboundPacket(curDevice, treatment, ByteBuffer.wrap(packet.serialize())))
                        case deviceId:DeviceId=>
                            packet.setSourceMACAddress(toMacAddress(curDevice))
                            packet.setDestinationMACAddress(toMacAddress(deviceId))
                            val treatment = DefaultTrafficTreatment.builder()
                                .setOutput(p.src().port())
                                .build()
                            packetService.emit(new DefaultOutboundPacket(curDevice, treatment, ByteBuffer.wrap(packet.serialize())))
                    }
                })
            }
        }

        def isControlPacket(ethPkt: Ethernet): Boolean = {
            ethPkt.getEtherType == Ethernet.TYPE_LLDP || ethPkt.getEtherType == Ethernet.TYPE_BSN
        }

        def isTargetPacket(packet: IPv4): Boolean = {
            val payload = packet.getPayload
            if (payload.isInstanceOf[UDP]) {
                true
            }
            else {
                false
            }
        }

        def packetOut(context: PacketContext): Unit = {
            context.send()
        }
    }

    class MyDeviceProcessor {

        def process(deviceId: DeviceId): Unit = {
            if (devices.contains(elem = deviceId)) {

            }
            else {
                // target jump
                val selectorBuilder = DefaultTrafficSelector.builder()
                selectorBuilder
                    .matchEthType(Ethernet.TYPE_IPV4)
                    .matchIPProtocol(17)
                val treatment = DefaultTrafficTreatment.builder()
                    .transition(3)
                    .build()
                val flowRule = DefaultFlowRule.builder()
                    .withSelector(selectorBuilder.build())
                    .withTreatment(treatment)
                    .withPriority(8000)
                    .fromApp(targetAppId)
                    .makeTemporary(2000)
                    .forTable(0)
                    .forDevice(deviceId)
                flowRuleService.applyFlowRules(flowRule.build())
                // fallback to controller
                val gotoControllor = DefaultTrafficTreatment.builder()
                    .punt()
                    .build()
                flowRuleService.applyFlowRules(DefaultFlowRule.builder()
                    .withSelector(selectorBuilder.build())
                    .withTreatment(gotoControllor)
                    .withPriority(0)
                    .fromApp(targetAppId)
                    .makeTemporary(2000)
                    .forTable(3)
                    .forDevice(deviceId)
                    .build())
                // broadcast intercept
                val broadcastSelector = DefaultTrafficSelector.builder()
                    .matchEthType(Ethernet.TYPE_IPV4)
                    .matchEthDst(MacAddress.BROADCAST)
                flowRuleService.applyFlowRules(DefaultFlowRule.builder()
                    .withSelector(broadcastSelector.build())
                    .withTreatment(gotoControllor)
                    .withPriority(8001)
                    .fromApp(appId)
                    .makeTemporary(2000)
                    .forTable(0)
                    .forDevice(deviceId)
                    .build())
                devices += deviceId
            }
        }
    }

    case class MonitorStatus(path: Path, intent: Intent, src: Host, dst: Host, intentBuilder: PointToPointIntent.Builder, target: Boolean)

    class MyLinkWeigher extends LinkWeigher {
        override def weight(edge: TopologyEdge): Weight = {
            var rate:Long = 1
            if(edge.link().src().elementId().isInstanceOf[DeviceId]) {
                val srcId = edge.link().src().deviceId()
                if(deviceRates.contains(srcId)) {
                    if(deviceRates(srcId)>0){
                        log.info(f"weigher device == ${srcId}")
                    }
                    rate += deviceRates(srcId)
                }
            }
            if(edge.link().dst().elementId().isInstanceOf[DeviceId]) {
                val dstId = edge.link().dst().deviceId()
                if(deviceRates.contains(dstId)) {
                    if(deviceRates(dstId)>0){
                        log.info(f"weigher device == ${dstId}")
                    }
                    rate += deviceRates(dstId)
                }
            }
            new ScalarWeight(rate)
        }

        override def getInitialWeight: Weight = {
            new ScalarWeight(1)
        }

        override def getNonViableWeight: Weight = {
            new ScalarWeight(1)
        }
    }

    class EmptyLinkWeigher extends LinkWeigher {
        override def weight(edge: TopologyEdge): Weight = {
            new ScalarWeight(1)
        }

        override def getInitialWeight: Weight = {
            new ScalarWeight(1)
        }

        override def getNonViableWeight: Weight = {
            new ScalarWeight(1)
        }
    }

    class PortStatsTask {
        private var exit: Boolean = false

        def schedule(): Unit = {
            timer.scheduleAtFixedRate(new Task(), 0, 1000)
        }

        def isExit: Boolean = {
            exit
        }

        def cancel(): Unit = {
            exit = true
            timer.cancel()
        }

        class Task extends TimerTask {
            override def run(): Unit = {
//                deviceService.getDevices.forEach(device=>{
//                    val load = flowStatService.loadSummary(device,PortNumber.IN_PORT)
//                    val rate = load.totalLoad().rate()
//                    log.info(f"device ${device.id()}, rate == $rate")
//                    deviceRates.put(device.id(),load.totalLoad().rate())
//                })
                deviceService.getDevices.forEach(device=>{
                    val load = flowStatService.loadSummary(device,PortNumber.IN_PORT)
//                    log.info("port number == in_port")
                    val rate = load.unknownLoad().rate()
//                    if(rate!=0) {
//                        log.info(f"device == ${device.id()}, totalLoad == ${load.totalLoad()}, unknownLoad == ${load.unknownLoad()}")
//                    }
                    deviceRates.put(device.id(),rate)
                })
                intents.forEach((key, m) => {
                    val newPathIter = if (m.target) {
                        pathService.getPaths(m.src.id(), m.dst.id(), linkWeigher).iterator().asScala.toList
                    }
                    else {
                        pathService.getPaths(m.src.id(), m.dst.id(), emptyWeigher).iterator().asScala.toList
                    }
                    if (newPathIter.nonEmpty) {
                        val path = newPathIter.head
                        if(m.target) {
//                            for(link <- newPathIter) {
//                                log.info(f"link cost == ${link.weight()}, old == ${link.equals(m.path)}")
//                            }
                        }
                        if (!path.equals(m.path)) {
                            log.info(f"switch...")
                            log.info(f"path == $path")
                            val builder: PointToPointIntent.Builder = m.intentBuilder
                            builder.suggestedPath(path.links().subList(1, path.links().size() - 1))
                            val intent = builder.build()
                            intents.replace(key, MonitorStatus(path, intent, src = m.src, dst = m.dst, m.intentBuilder, target = m.target))
                            intentService.submit(intent)
                        }
                    }
                })
            }
        }

    }

}