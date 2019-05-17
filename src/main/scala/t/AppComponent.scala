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
import java.util.{Timer, TimerTask}

import scala.util.control.Breaks._
import org.onlab.packet._
import org.onosproject.core.{ApplicationId, CoreService}
import org.onosproject.net._
import org.onosproject.net.flow._
import org.onosproject.net.flow.instructions.{Instruction, Instructions}
import org.onosproject.net.flowobjective.{DefaultForwardingObjective, FlowObjectiveService, ForwardingObjective}
import org.onosproject.net.host.HostService
import org.onosproject.net.packet._
import org.onosproject.net.topology.{LinkWeigher, PathService, TopologyEdge, TopologyEvent, TopologyListener, TopologyService}
import org.osgi.service.component.annotations.Activate
import org.apache.felix.scr.annotations.Component
import org.osgi.service.component.annotations.Deactivate
import org.slf4j.LoggerFactory
import org.apache.felix.scr.annotations.Reference
import org.apache.felix.scr.annotations.ReferenceCardinality
import org.onlab.graph.{ScalarWeight, Weight}
import org.onosproject.cfg.ComponentConfigService
import org.onosproject.net.device.{DeviceEvent, DeviceListener, DeviceService}
import org.onosproject.net.flow.criteria.{Criterion, EthCriterion, MetadataCriterion}
import org.onosproject.net.link.{DefaultLinkDescription, LinkEvent, LinkListener, LinkProvider, LinkProviderRegistry, LinkProviderService, LinkService, LinkStore}
import org.osgi.service.component.ComponentContext
import org.onlab.packet.UDP
import org.onlab.util.DataRateUnit
import org.onosproject.net.intent.constraint.{AnnotationConstraint, BandwidthConstraint}
import org.onosproject.net.intent.{Constraint, HostToHostIntent, Intent, IntentCompiler, IntentExtensionService, IntentService, IntentStore, Key, LinkCollectionIntent, PointToPointIntent}
import org.onosproject.net.provider.ProviderId
import org.onosproject.net.statistic.{FlowStatisticService, PortStatisticsService, StatisticService}
import t.Intent.{Ipv4Constraint, LinkCollectionIntentCompiler}

import scala.collection.{JavaConverters, mutable}
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer


/**
 * Skeletal ONOS application component.
 */
@Component(immediate = true)
class AppComponent {
    final private val log = LoggerFactory.getLogger(getClass)
    val topologyListener = new MyTopologyListener
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
    var packetProcessor = new MyPacketProcessor
    var devices: Set[DeviceId] = Set.empty
    var task: Option[PortStatsTask] = None
    var targetStatus: Map[DeviceId, PortStatus] = Map.empty
    var old_compiler: Option[IntentCompiler[LinkCollectionIntent]] = None
    var intents: java.util.concurrent.ConcurrentHashMap[Key, MonitorStatus] = new java.util.concurrent.ConcurrentHashMap()
    var linkRates: mutable.HashMap[Link, Long] = mutable.HashMap.empty
    val timer = new Timer()
    val linkWeigher = new MyLinkWeigher

    @Activate def activate(context: ComponentContext): Unit = {
        //cfgService.registerProperties(getClass)
        appId = coreService.registerApplication("test scala")
        packetService.addProcessor(packetProcessor, PacketProcessor.director(2))
        //        topologyService.addListener(topologyListener)
        requestIntercepts()
//        linkService.addListener(event => {
//            log.info(f"link event == $event")
//        })
        intentService.getIntents.forEach(intent=>{
            if(intent.appId()==appId) {
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
        //cfgService.unregisterProperties(getClass, false)
        withdrawIntercepts()
        linkProviderRegistry.unregister(this)
        intents.forEach((_, m)=>{
            intentService.withdraw(m.intent)
            intentService.purge(m.intent)
        })
        flowRuleService.removeFlowRulesById(appId)
        packetService.removeProcessor(packetProcessor)
        //        topologyService.removeListener(topologyListener)
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

    class MyTopologyListener extends TopologyListener {
        final private val log = LoggerFactory.getLogger(getClass)

        /*
        *  For one link removed,
        *  Two LINK_REMOVED events will be fired from two directions
        */
        override def event(event: TopologyEvent): Unit = {
            val reasons = JavaConverters.asScalaBuffer(event.reasons())
            if (reasons != null) {
                reasons.foreach {
                    case le: LinkEvent if le.`type`() == LinkEvent.Type.LINK_REMOVED =>
                        fixBlackHole(le.subject().src())
                    case _ =>
                }
            }
        }

        def fixBlackHole(egress: ConnectPoint): Unit = {
            val rules = getFlowRulesFrom(egress)
            val pairs = findSrcDstPairs(rules)
            val srcPaths = scala.collection.mutable.HashMap.empty[DeviceId, mutable.Set[Path]]
            pairs.foreach(p => {
                val srcHost = hostService.getHost(HostId.hostId(p._1))
                val dstHost = hostService.getHost(HostId.hostId(p._2))
                if (srcHost != null && dstHost != null) {
                    val srcId = srcHost.location().deviceId()
                    val dstId = dstHost.location().deviceId()
                    var shortestPaths = srcPaths.get(dstId)
                    if (shortestPaths.isEmpty) {
                        shortestPaths = Some(JavaConverters.asScalaSet(topologyService.getPaths(topologyService.currentTopology(), egress.deviceId(), dstId)))
                        srcPaths.put(dstId, shortestPaths.get)
                    }
                    backTrackBadNodes(shortestPaths.get, srcId, p)
                }
            })
        }

        // 1 -> 2 -> 3
        def backTrackBadNodes(shortestPaths: mutable.Set[Path], dstId: DeviceId, sd: (MacAddress, MacAddress)): Unit = {
            shortestPaths.foreach(p => {
                val pathLinks = p.links()
                breakable({
                    for (i <- 0 until pathLinks.size()) {
                        val curLink = pathLinks.get(i)
                        val curDevice = curLink.src().deviceId()

                        // clean
                        cleanFlowRules(sd, curDevice)

                        val pathsFromCurDevice = topologyService.getPaths(topologyService.currentTopology(), curDevice, dstId)
                        if (pickForwardPathIfPossible(pathsFromCurDevice, curLink.src().port()).isDefined) {
                            break
                        }
                        else {
                            if (i + 1 == pathLinks.size()) {
                                cleanFlowRules(sd, curLink.dst().deviceId())
                            }
                        }
                    }
                })
            })
        }

        def cleanFlowRules(pair: (MacAddress, MacAddress), id: DeviceId): Unit = {
            flowRuleService.getFlowEntries(id).forEach(r => {
                var matchesSrc = false
                var matchesDst = false
                r.treatment().allInstructions().forEach(i => {
                    if (i.`type`.equals(Instruction.Type.OUTPUT)) { // if the flow has matching src and dst
                        r.selector().criteria().forEach(cr => {
                            if (cr.`type`().equals(Criterion.Type.ETH_DST)) {
                                if (cr.asInstanceOf[EthCriterion].mac().equals(pair._2)) {
                                    matchesDst = true
                                }
                            }
                            else if (cr.`type`().equals(Criterion.Type.ETH_SRC)) {
                                if (cr.asInstanceOf[EthCriterion].mac().equals(pair._1)) {
                                    matchesSrc = true
                                }
                            }
                        })
                    }
                })
                if (matchesDst && matchesSrc) {
                    log.info("Removed flow rule from device == {}", id)
                    flowRuleService.removeFlowRules(r.asInstanceOf[FlowRule])
                }
            })
        }

        def pickForwardPathIfPossible(paths: util.Set[Path], number: PortNumber): Option[Path] = {
            var lastPath: Option[Path] = None
            val t = JavaConverters.asScalaSet(paths).flatMap(p => {
                lastPath = Some(p)
                if (!p.src().port().equals(number)) {
                    Some(p)
                }
                else {
                    None
                }
            })
            t.headOption.orElse(lastPath)
        }

        def getFlowRulesFrom(egress: ConnectPoint): List[FlowEntry] = {
            JavaConverters.asScalaIterator(flowRuleService.getFlowEntries(egress.deviceId()).iterator())
                .filter(_.appId() == appId.id())
                .flatMap(p => {
                    JavaConverters.asScalaBuffer(p.treatment().allInstructions())
                        .filter(_.`type`() == Instruction.Type.OUTPUT)
                        .flatMap({
                            case b: Instructions.OutputInstruction if b.port().equals(egress.port()) => Some(p)
                            case _ => None
                        })
                })
                .toList
        }

        def findSrcDstPairs(rules: List[FlowEntry]): List[(MacAddress, MacAddress)] = {
            rules.map(r => {
                var src: MacAddress = null
                var dst: MacAddress = null
                r.selector().criteria().forEach(cr => {
                    if (cr.`type`() == Criterion.Type.ETH_DST) {
                        dst = cr.asInstanceOf[EthCriterion].mac()
                    }
                    else if (cr.`type`() == Criterion.Type.ETH_SRC) {
                        src = cr.asInstanceOf[EthCriterion].mac()
                    }
                })
                (src, dst)
            })
        }
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
                if (topologyService.isBroadcastPoint(topologyService.currentTopology(), context.inPacket().receivedFrom())) {
                    packetOut(context, PortNumber.FLOOD)
                    return
                }
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

            setConnectivity(srcHost, dstHost, srcIp, dstIp, constraints, target = false)
            context.
            return

            val dstDeviceId = dstHost.location().deviceId()
            val srcDeviceId = srcHost.location().deviceId()
            val curDeviceId = inPacket.receivedFrom().deviceId()
            val curMac = toMacAddress(curDeviceId)

            if (srcDeviceId == dstDeviceId) {
                if (srcDeviceId == curDeviceId) {
                    val outPort = findHostOutport(curDeviceId, dstHost.id()).get
                    val selectorBuilder_Out = DefaultTrafficSelector.builder()
                    val ipv4srcMatch = Ip4Prefix.valueOf(srcIp, Ip4Prefix.MAX_MASK_LENGTH)
                    val ipv4dstMatch = Ip4Prefix.valueOf(dstIp, Ip4Prefix.MAX_MASK_LENGTH)
                    selectorBuilder_Out.matchEthDst(dstHost.mac())
                        .matchEthSrc(srcHost.mac())
                        .matchIPSrc(ipv4srcMatch)
                        .matchIPDst(ipv4dstMatch)
                        .matchEthType(Ethernet.TYPE_IPV4)
                    val treatment_Out = DefaultTrafficTreatment.builder()
                        .setOutput(outPort)
                        .build()
                    val forwardingObjective_Out = DefaultForwardingObjective.builder()
                        .withSelector(selectorBuilder_Out.build())
                        .withTreatment(treatment_Out)
                        .withPriority(10)
                        .fromApp(appId)
                        .withFlag(ForwardingObjective.Flag.VERSATILE)
                        .makeTemporary(2000)
                        .add()
                    flowObjectiveService.forward(curDeviceId, forwardingObjective_Out)
                    val selectorBuilder_In = DefaultTrafficSelector.builder()
                    selectorBuilder_In.matchEthDst(dstHost.mac())
                        .matchEthSrc(srcHost.mac())
                        .matchIPSrc(ipv4dstMatch)
                        .matchIPDst(ipv4srcMatch)
                        .matchEthType(Ethernet.TYPE_IPV4)
                    val treatment_In = DefaultTrafficTreatment.builder()
                        .setOutput(context.inPacket().receivedFrom().port())
                        .build()
                    val forwardingObjective_In = DefaultForwardingObjective.builder()
                        .withSelector(selectorBuilder_In.build())
                        .withTreatment(treatment_In)
                        .withPriority(10)
                        .fromApp(appId)
                        .withFlag(ForwardingObjective.Flag.VERSATILE)
                        .makeTemporary(2000)
                        .add()
                    flowObjectiveService.forward(curDeviceId, forwardingObjective_In)
                    packetOut(context, outPort)
                    return
                }
                else {
                    log.info("should not happen")
                }
            }

            // handle src edge packet
            if (curDeviceId == srcDeviceId) {
                log.info("at source edge node")
                findNextHopDevice(curDeviceId, srcDeviceId, dstDeviceId) match {
                    case None =>
                        log.info("find none hop")
                    case Some(nexthop) =>
                        installInRule(srcIp, ethPacket.getSourceMAC, dstIp, ethPacket.getDestinationMAC, inPacket.receivedFrom().port(), curDeviceId, curMac, toMacAddress(nexthop._1))
                        installOutRule(srcIp, ethPacket.getSourceMAC, dstIp, ethPacket.getDestinationMAC, nexthop._2, curDeviceId, curMac, toMacAddress(nexthop._1), context)
                }
            }
            else if (curDeviceId == dstDeviceId) {
                log.info("at dest edge node == {}", dstDeviceId.toString)
                val hostOutport = findHostOutport(curDeviceId, dstHost.id()).get
                log.info("host out port == {}", hostOutport.toString)
                // install out rule
                val srcMac = ethPacket.getSourceMAC
                installOutRule(srcIp, srcMac, dstIp, curMac, hostOutport, curDeviceId, srcHost.mac(), dstHost.mac(), context)
                // install in rule
                installInRule(srcIp, srcMac, dstIp, curMac, context.inPacket().receivedFrom().port(), curDeviceId, srcHost.mac(), dstHost.mac())
            }
            else {
                log.info("at inter node")
                log.info("src == {}", srcDeviceId.toString)
                log.info("dst == {}", dstDeviceId.toString)
                log.info("cur == {}", curDeviceId.toString)
                findNextHopDevice(curDeviceId, srcDeviceId, dstDeviceId) match {
                    case None =>
                        log.info("find none hop")
                    case Some(nexthop) =>
                        val inPort = context.inPacket().receivedFrom().port()
                        log.info("inPort == {}", inPort.toString)
                        // install out rule
                        val srcMac = ethPacket.getSourceMAC
                        // for ethernet test:
                        installOutRule(srcIp, srcMac, dstIp, curMac, nexthop._2, curDeviceId, curMac, toMacAddress(nexthop._1), context)
                        // for wireless:
                        // installOutRule(srcIp, srcMac, dstIp, curMac, PortNumber.IN_PORT, curDeviceId, curMac, toMacAddress(nexthop._1), context)

                        // install in rule
                        // for wireless:
                        // installInRule(srcIp, srcMac, dstIp, curMac, PortNumber.IN_PORT, curDeviceId, curMac, toMacAddress(nexthop._1))
                        // for ethernet test:
                        installInRule(srcIp, srcMac, dstIp, curMac, inPort, curDeviceId, curMac, toMacAddress(nexthop._1))
                }
            }
        }

        def setConnectivity(srcHost: Host, dstHost: Host, srcIp: Ip4Address, dstIp: Ip4Address, constraints: util.List[Constraint], target:Boolean): Unit = {
            val key = Key.of(f"${srcHost.id()},${dstHost.id()},$target", appId)
            if (intents.contains(key)) {
                return
            }
            else {
                val path = pathService.getPaths(srcHost.id(), dstHost.id()).iterator().next()
                if (path == null) {
                    log.info(f"no path found")
                    return
                }
                val firstLink = path.links().get(0)
                val lastLink = path.links().get(path.links().size() - 1)
                val intentBuilder = PointToPointIntent.builder()
                    .filteredIngressPoint(new FilteredConnectPoint(firstLink.dst()))
                    .filteredEgressPoint(new FilteredConnectPoint(lastLink.src()))
                    .constraints(constraints)
                    .priority(15)
                    .appId(appId)
                    .key(key)
                    .suggestedPath(path.links().subList(1, path.links().size() - 2))
                val intent = intentBuilder.build()
                intents.put(key, MonitorStatus(path, intent, src = srcHost, dst = dstHost, intentBuilder, target))
                //intents.put(key, MonitorStatus(path, intent, src = srcHost, dst = dstHost, intentBuilder))
                intentService.submit(intent)
            }
        }

        def findNextHopDevice(currentDeviceId: DeviceId, srcDeviceId: DeviceId, dstDeviceId: DeviceId): Option[(DeviceId, PortNumber)] = {
            val paths = topologyService.getPaths(topologyService.currentTopology(), currentDeviceId, dstDeviceId)
            log.info("find cur deviceId == {}", currentDeviceId)
            log.info("find dst deviceId == {}", dstDeviceId)
            JavaConverters.asScalaSet(paths).find(p => {
                p.dst().deviceId() != srcDeviceId
            }).map(_.links().get(0))
                .map(l => {
                    (l.dst().deviceId(), l.src().port())
                })
        }

        def findHostOutport(currentDeviceId: DeviceId, hostId: HostId): Option[PortNumber] = {
            val paths = pathService.getPaths(currentDeviceId, hostId)
            val p: mutable.Set[PortNumber] = JavaConverters.asScalaSet(paths).map(_.links().get(0))
                .map(l => l.src().port())
            p.headOption
        }

        def installInRule(srcIp: Ip4Address, srcMac: MacAddress, dstIp: Ip4Address, dstMac: MacAddress, inPort: PortNumber, curDeviceId: DeviceId, curMac: MacAddress, nextHopMac: MacAddress, priority: Int = 10): Unit = {
            val selectorBuilder = DefaultTrafficSelector.builder()
            val ipv4srcMatch = Ip4Prefix.valueOf(srcIp, Ip4Prefix.MAX_MASK_LENGTH)
            val ipv4dstMatch = Ip4Prefix.valueOf(dstIp, Ip4Prefix.MAX_MASK_LENGTH)
            selectorBuilder.matchEthDst(curMac)
                .matchEthSrc(nextHopMac)
                .matchIPSrc(ipv4dstMatch)
                .matchIPDst(ipv4srcMatch)
                .matchEthType(Ethernet.TYPE_IPV4)

            val treatment = DefaultTrafficTreatment.builder()
                .setEthDst(srcMac)
                .setEthSrc(dstMac)
                .setOutput(inPort)
                .build()
            val forwardingObjective = DefaultFlowRule.builder()
                .withSelector(selectorBuilder.build())
                .withTreatment(treatment)
                .withPriority(priority)
                .fromApp(appId)
                .forTable(0)
                .makeTemporary(2000)
                .forDevice(curDeviceId)
                .build()
            log.info("install in rule == {}", forwardingObjective.toString)
            flowRuleService.applyFlowRules(forwardingObjective)
        }

        def installOutRule(srcIp: Ip4Address, srcMac: MacAddress, dstIp: Ip4Address, dstMac: MacAddress, outPort: PortNumber, curDeviceId: DeviceId, curMac: MacAddress, nextHopMac: MacAddress, context: PacketContext, priority: Int = 10): Unit = {
            val selectorBuilder = DefaultTrafficSelector.builder()
            val ipv4srcMatch = Ip4Prefix.valueOf(srcIp, Ip4Prefix.MAX_MASK_LENGTH)
            val ipv4dstMatch = Ip4Prefix.valueOf(dstIp, Ip4Prefix.MAX_MASK_LENGTH)
            selectorBuilder.matchEthDst(dstMac)
                .matchEthSrc(srcMac)
                .matchIPSrc(ipv4srcMatch)
                .matchIPDst(ipv4dstMatch)
                .matchEthType(Ethernet.TYPE_IPV4)

            val treatment = DefaultTrafficTreatment.builder()
                .setEthSrc(curMac)
                .setEthDst(nextHopMac)
                .setOutput(outPort)
                .build()
            val forwardingObjective = DefaultFlowRule.builder()
                .withSelector(selectorBuilder.build())
                .withTreatment(treatment)
                .withPriority(priority)
                .fromApp(appId)
                .forTable(0)
                .makeTemporary(2000)
                .forDevice(curDeviceId)
                .build()
            log.info("install out rule == {}", forwardingObjective.toString)
            flowRuleService.applyFlowRules(forwardingObjective)

            val packet = context.inPacket().parsed()
            packet.setDestinationMACAddress(nextHopMac)
            packet.setSourceMACAddress(curMac)
            packetService.emit(new DefaultOutboundPacket(curDeviceId, treatment, ByteBuffer.wrap(packet.serialize())))
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

        def packetOut(context: PacketContext, number: PortNumber): Unit = {
            context.treatmentBuilder().setOutput(number)
            context.send()
        }

        def handleTargetPacket(context: PacketContext, inPacket: InboundPacket): Unit = {
            log.info("target!!!")
            log.info(f"$targetStatus")
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

            val dstDeviceId = dstHost.location().deviceId()
            val srcDeviceId = srcHost.location().deviceId()
            val curDeviceId = inPacket.receivedFrom().deviceId()
            val curMac = toMacAddress(curDeviceId)

            if (srcDeviceId == dstDeviceId) {
                if (srcDeviceId == curDeviceId) {
                    val outPort = findHostOutport(curDeviceId, dstHost.id()).get
                    val selectorBuilder_Out = DefaultTrafficSelector.builder()
                    val ipv4srcMatch = Ip4Prefix.valueOf(srcIp, Ip4Prefix.MAX_MASK_LENGTH)
                    val ipv4dstMatch = Ip4Prefix.valueOf(dstIp, Ip4Prefix.MAX_MASK_LENGTH)
                    selectorBuilder_Out.matchEthDst(dstHost.mac())
                        .matchEthSrc(srcHost.mac())
                        .matchIPSrc(ipv4srcMatch)
                        .matchIPDst(ipv4dstMatch)
                        .matchEthType(Ethernet.TYPE_IPV4)
                    val treatment_Out = DefaultTrafficTreatment.builder()
                        .setOutput(outPort)
                        .build()
                    val forwardingObjective_Out = DefaultForwardingObjective.builder()
                        .withSelector(selectorBuilder_Out.build())
                        .withTreatment(treatment_Out)
                        .withPriority(10)
                        .fromApp(appId)
                        .withFlag(ForwardingObjective.Flag.VERSATILE)
                        .makeTemporary(2000)
                        .add()
                    flowObjectiveService.forward(curDeviceId, forwardingObjective_Out)
                    val selectorBuilder_In = DefaultTrafficSelector.builder()
                    selectorBuilder_In.matchEthDst(dstHost.mac())
                        .matchEthSrc(srcHost.mac())
                        .matchIPSrc(ipv4dstMatch)
                        .matchIPDst(ipv4srcMatch)
                        .matchEthType(Ethernet.TYPE_IPV4)
                    val treatment_In = DefaultTrafficTreatment.builder()
                        .setOutput(context.inPacket().receivedFrom().port())
                        .build()
                    val forwardingObjective_In = DefaultForwardingObjective.builder()
                        .withSelector(selectorBuilder_In.build())
                        .withTreatment(treatment_In)
                        .withPriority(10)
                        .fromApp(appId)
                        .withFlag(ForwardingObjective.Flag.VERSATILE)
                        .makeTemporary(2000)
                        .add()
                    flowObjectiveService.forward(curDeviceId, forwardingObjective_In)
                    packetOut(context, outPort)
                    return
                }
                else {
                    log.info("should not happen")
                }
            }

            // handle src edge packet
            if (curDeviceId == srcDeviceId) {
                log.info("at source edge node")
                val result = findNextHops(curDeviceId, srcDeviceId, dstDeviceId)
                val inPort = context.inPacket().receivedFrom().port()
                val priority = 80
                val srcMac = ethPacket.getSourceMAC
                result match {
                    case s :: rest => {
                        var tableId = 3
                        var pathTables = new ListBuffer[Int]()
                        pathTables += tableId
                        val dstMac = ethPacket.getDestinationMAC
                        installTargetOutRule(srcIp, srcMac, dstIp, dstMac, s._2, curDeviceId, curMac, toMacAddress(s._1), context, priority, tableId, send_immedidately = true)
                        //                        installTargetInRule(srcIp, srcMac, dstIp, ethPacket.getDestinationMAC, inPort, curDeviceId, curMac, toMacAddress(s._1), priority, tableId = 3)
                        //                        installInRule(srcIp, ethPacket.getSourceMAC, dstIp, ethPacket.getDestinationMAC, inPacket.receivedFrom().port(), curDeviceId, curMac, toMacAddress(nexthop._1),priority,target = true)
                        //                        installOutRule(srcIp, ethPacket.getSourceMAC, dstIp, ethPacket.getDestinationMAC, nexthop._2, curDeviceId, curMac, toMacAddress(nexthop._1), context,priority,target = true)
                        tableId += 1
                        rest.foreach(nexthop => {
                            // install out rule
                            installTargetOutRule(srcIp, srcMac, dstIp, dstMac, nexthop._2, curDeviceId, curMac, toMacAddress(nexthop._1), context, priority, tableId, send_immedidately = false)
                            //                            installTargetInRule(srcIp, srcMac, dstIp, ethPacket.getDestinationMAC, inPort, curDeviceId, curMac, toMacAddress(nexthop._1), priority, tableId)
                            pathTables += tableId
                            tableId += 1
                        })
                        targetStatus += (context.inPacket().receivedFrom().deviceId() -> PortStatus(3, s._2, pathTables.toList, 3))
                    }
                    case Nil => {
                        log.info("find none hop")
                    }
                }
            }
            else if (curDeviceId == dstDeviceId) {
                log.info(f"at dest edge node == ${dstDeviceId.toString}, connectPoint == ${inPacket.receivedFrom()}")
                val hostOutport = findHostOutport(curDeviceId, dstHost.id()).get
                // install out rule
                val srcMac = ethPacket.getSourceMAC
                val curTableId = if (targetStatus.contains(inPacket.receivedFrom().deviceId())) {
                    val cur = targetStatus(inPacket.receivedFrom().deviceId()).currentTableId
                    log.info("current table id == {}", cur)
                    cur
                }
                else {
                    3
                }
                installTargetOutRule(srcIp, srcMac, dstIp, curMac, hostOutport, curDeviceId, srcHost.mac(), dstHost.mac(), context, tableId = curTableId, send_immedidately = true)
                // install in rule
                //                installTargetInRule(srcIp, srcMac, dstIp, curMac, context.inPacket().receivedFrom().port(), curDeviceId, srcHost.mac(), dstHost.mac(), tableId = 3)
            }
            else {
                log.info("at inter node")
                log.info("src == {}", srcDeviceId.toString)
                log.info("dst == {}", dstDeviceId.toString)
                log.info("cur == {}", curDeviceId.toString)
                val inPort = context.inPacket().receivedFrom().port()
                val result = findNextHops(curDeviceId, srcDeviceId, dstDeviceId)
                val priority = 80
                val srcMac = ethPacket.getSourceMAC
                result match {
                    case s :: rest => {
                        var tableId = 3
                        var pathTables = new ListBuffer[Int]()
                        pathTables += tableId
                        installTargetOutRule(srcIp, srcMac, dstIp, curMac, s._2, curDeviceId, curMac, toMacAddress(s._1), context, priority, tableId, send_immedidately = true)
                        //                        installTargetInRule(srcIp, srcMac, dstIp, curMac, inPort, curDeviceId, curMac, toMacAddress(s._1), priority, tableId = 3)
                        tableId += 1
                        rest.foreach(nexthop => {
                            // install out rule
                            installTargetOutRule(srcIp, srcMac, dstIp, curMac, nexthop._2, curDeviceId, curMac, toMacAddress(nexthop._1), context, priority, tableId, send_immedidately = false)
                            //                            installTargetInRule(srcIp, srcMac, dstIp, curMac, inPort, curDeviceId, curMac, toMacAddress(nexthop._1), priority, tableId)
                            pathTables += tableId
                            tableId += 1
                        })
                        targetStatus += (context.inPacket().receivedFrom().deviceId() -> PortStatus(3, s._2, pathTables.toList, 3))
                    }
                    case Nil => {
                        log.info("find none hop")
                    }
                }
            }
        }

        def findNextHops(currentDeviceId: DeviceId, srcDeviceId: DeviceId, dstDeviceId: DeviceId): List[(DeviceId, PortNumber)] = {
            val paths: Iterator[Path] = topologyService.getKShortestPaths(topologyService.currentTopology(), currentDeviceId, dstDeviceId).iterator().asScala
            paths.filter(p => {
                p.dst().deviceId() != srcDeviceId
            }).map(_.links().get(0))
                .map(l => {
                    (l.dst().deviceId(), l.src().port())
                }).toList
        }

        def installTargetInRule(srcIp: Ip4Address, srcMac: MacAddress, dstIp: Ip4Address, dstMac: MacAddress, inPort: PortNumber, curDeviceId: DeviceId, curMac: MacAddress, nextHopMac: MacAddress, priority: Int = 10, tableId: Int): Unit = {
            val selectorBuilder = DefaultTrafficSelector.builder()
            val ipv4srcMatch = Ip4Prefix.valueOf(srcIp, Ip4Prefix.MAX_MASK_LENGTH)
            val ipv4dstMatch = Ip4Prefix.valueOf(dstIp, Ip4Prefix.MAX_MASK_LENGTH)
            selectorBuilder.matchEthDst(curMac)
                .matchEthSrc(nextHopMac)
                .matchIPSrc(ipv4dstMatch)
                .matchIPDst(ipv4srcMatch)
                .matchEthType(Ethernet.TYPE_IPV4)

            val treatment = DefaultTrafficTreatment.builder()
                .setEthDst(srcMac)
                .setEthSrc(dstMac)
                .setOutput(inPort)
                .build()
            val forwardingObjective = DefaultFlowRule.builder()
                .withSelector(selectorBuilder.build())
                .withTreatment(treatment)
                .withPriority(priority)
                .fromApp(appId)
                .forTable(tableId)
                .makeTemporary(2000)
                .forDevice(curDeviceId)
                .build()
            log.info("install in rule == {}", forwardingObjective.toString)
            flowRuleService.applyFlowRules(forwardingObjective)
        }

        def installTargetOutRule(srcIp: Ip4Address, srcMac: MacAddress, dstIp: Ip4Address, dstMac: MacAddress, outPort: PortNumber, curDeviceId: DeviceId, curMac: MacAddress, nextHopMac: MacAddress, context: PacketContext, priority: Int = 10, tableId: Int, send_immedidately: Boolean): Unit = {
            val selectorBuilder = DefaultTrafficSelector.builder()
            val ipv4srcMatch = Ip4Prefix.valueOf(srcIp, Ip4Prefix.MAX_MASK_LENGTH)
            val ipv4dstMatch = Ip4Prefix.valueOf(dstIp, Ip4Prefix.MAX_MASK_LENGTH)
            selectorBuilder.matchEthDst(dstMac)
                .matchEthSrc(srcMac)
                .matchIPSrc(ipv4srcMatch)
                .matchIPDst(ipv4dstMatch)
                .matchEthType(Ethernet.TYPE_IPV4)

            val treatment = DefaultTrafficTreatment.builder()
                .setEthSrc(curMac)
                .setEthDst(nextHopMac)
                .setOutput(outPort)
                .wipeDeferred()
                .build()
            val forwardingObjective = DefaultFlowRule.builder()
                .withSelector(selectorBuilder.build())
                .withTreatment(treatment)
                .withPriority(priority)
                .fromApp(appId)
                .forTable(tableId)
                .makeTemporary(2000)
                .forDevice(curDeviceId)
                .build()
            log.info("install out rule == {}", forwardingObjective.toString)
            flowRuleService.applyFlowRules(forwardingObjective)

            if (send_immedidately) {
                val packet = context.inPacket().parsed()
                packet.setDestinationMACAddress(nextHopMac)
                packet.setSourceMACAddress(curMac)
                packetService.emit(new DefaultOutboundPacket(curDeviceId, treatment, ByteBuffer.wrap(packet.serialize())))
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

    def setTargetTableJump(deviceId: DeviceId, tableId: Int): Unit = {
        val old_tableId = targetStatus(deviceId).currentTableId
        log.info(f"old_status == $old_tableId")
        val old_punt: Option[FlowEntry] = flowRuleService.getFlowEntries(deviceId).iterator().asScala
            .filter(_.appId() == appId.id())
            .filter(_.table().compareTo(IndexTableId.of(old_tableId)) == 0)
            .find(_.treatment().allInstructions().asScala.exists(
                (p: Instruction) => {
                    if (p.`type`() == Instruction.Type.OUTPUT) {
                        p.asInstanceOf[Instructions.OutputInstruction].port().exactlyEquals(PortNumber.CONTROLLER)
                    }
                    else {
                        false
                    }
                }
            ))
        flowRuleService.getFlowEntries(deviceId).iterator().asScala
            .filter(_.appId() == appId.id())
            .foreach(p => {
                val is_del = p.treatment().tableTransition()
                if (is_del != null) {
                    val selectorBuilder = DefaultTrafficSelector.builder()
                    selectorBuilder
                        .matchEthType(Ethernet.TYPE_IPV4)
                        .matchIPProtocol(17)
                    flowRuleService.applyFlowRules(DefaultFlowRule.builder()
                        .withSelector(selectorBuilder.build())
                        .withTreatment(DefaultTrafficTreatment.builder()
                            .transition(tableId)
                            .build())
                        .withPriority(p.priority())
                        .fromApp(appId)
                        .makeTemporary(2000)
                        .forTable(0)
                        .forDevice(deviceId)
                        .build())
                    flowRuleService.applyFlowRules(DefaultFlowRule.builder()
                        .withSelector(selectorBuilder.build())
                        .withTreatment(DefaultTrafficTreatment.builder()
                            .punt()
                            .build())
                        .withPriority(0)
                        .fromApp(appId)
                        .makeTemporary(2000)
                        .forTable(tableId)
                        .forDevice(deviceId)
                        .build())
                    old_punt.foreach(old => {
                        flowRuleService.removeFlowRules(old)
                    })
                    val old = targetStatus(deviceId)
                    old.currentTableId = tableId
                    targetStatus += (deviceId -> old)
                }
            })
    }

    def selectSecondaryTargetJump(deviceId: DeviceId): Int = {
        val state = targetStatus(deviceId)
        val newPath = state.PathTableIds.find(p => {
            p != state.currentTableId
        })
        if (newPath.isDefined) {
            newPath.get
        }
        else {
            log.info("no new path find.. using master path")
            state.MasterPathTableId
        }
    }

    def getMasterPathTableStats(deviceId: DeviceId): Long = {
        val masterPort = targetStatus(deviceId).MasterPathPort
        flowStatService.loadSummary(deviceService.getDevice(deviceId), masterPort).totalLoad().rate()
    }

    class MyDeviceProcessor {

        def process(deviceId: DeviceId): Unit = {
            if (devices.contains(elem = deviceId)) {

            }
            else {
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
                    .fromApp(appId)
                    .makeTemporary(2000)
                    .forTable(0)
                    .forDevice(deviceId)
                flowRuleService.applyFlowRules(flowRule.build())
                //
                val gotoControllor = DefaultTrafficTreatment.builder()
                    .punt()
                    .build()
                flowRuleService.applyFlowRules(DefaultFlowRule.builder()
                    .withSelector(selectorBuilder.build())
                    .withTreatment(gotoControllor)
                    .withPriority(0)
                    .fromApp(appId)
                    .makeTemporary(2000)
                    .forTable(3)
                    .forDevice(deviceId)
                    .build())
                devices += deviceId
            }
        }
    }

    case class PortStatus(MasterPathTableId: Int, MasterPathPort: PortNumber, PathTableIds: List[Int], var currentTableId: Int)

    case class MonitorStatus(path: Path, intent: Intent, src: Host, dst: Host, intentBuilder: PointToPointIntent.Builder, target:Boolean)

    class MyLinkWeigher extends LinkWeigher {
        override def weight(edge: TopologyEdge): Weight = {
            if(linkRates.contains(edge.link())) {
                new ScalarWeight(linkRates(edge.link()))
            }
            else {
                new ScalarWeight(1)
            }
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

        def schedule() = {
            timer.scheduleAtFixedRate(new Task(), 0, 1000)
        }

        def isExit(): Boolean = {
            exit
        }

        def cancel(): Unit = {
            exit = true
            timer.cancel()
        }

        class Task extends TimerTask {
            override def run(): Unit = {
                linkService.getLinks().forEach(link => {
                    linkRates.put(link, statService.load(link).rate())
                })
                intents.forEach((key,m)=>{
                    val newPathIter = pathService.getPaths(m.src.id(),m.dst.id(),linkWeigher).iterator()
                    if(newPathIter.hasNext){
                        val path = newPathIter.next()
                        if(!path.equals(m.path)) {
                            log.info(f"switch...")
                            log.info(f"path == $path")
                        }
                    }
                })
                //                intents.forEach((key,m)=>{
                //                    log.info(f"checking...$key")
                //                    val builder: PointToPointIntent.Builder = m.intentBuilder
                //                    val newPathIter = pathService.getKShortestPaths(m.src.id(), m.dst.id()).iterator()
                //                    newPathIter.next()
                //                    if(newPathIter.hasNext) {
                //                        val path = newPathIter.next()
                //                        if(!path.equals(m.path)) {
                //                            log.info(f"switch...")
                //                            log.info(f"path == $path")
                //                            builder.suggestedPath(path.links().subList(1, path.links().size() - 1))
                //                            val intent = builder.build()
                //                            intents.replace(key,MonitorStatus(path, intent, src = m.src, dst = m.dst, m.intentBuilder))
                //                            intentService.submit(intent)
                //                        }
                //                    }
                //                })
            }

            //                targetStatus.foreach(what => {
            //                    val deviceId = what._1
            //                    val status = what._2
            //                    val rate = getMasterPathTableStats(deviceId)
            //                    val info = f"connect point $deviceId rate == $rate"
            //                    log.info(info)
            //                    if (rate > 1000 && status.currentTableId == status.MasterPathTableId) {
            //                        log.info(f"switching...-- $deviceId")
            //                        val tableId = selectSecondaryTargetJump(deviceId)
            //                        setTargetTableJump(deviceId, tableId)
            //                    }
            //                })
        }

    }
}