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

import scala.util.control.Breaks._
import org.onlab.packet._
import org.onosproject.core.{ApplicationId, CoreService}
import org.onosproject.net._
import org.onosproject.net.flow._
import org.onosproject.net.flow.instructions.{Instruction, Instructions}
import org.onosproject.net.flowobjective.{DefaultForwardingObjective, FlowObjectiveService, ForwardingObjective}
import org.onosproject.net.host.HostService
import org.onosproject.net.packet._
import org.onosproject.net.topology.{PathService, TopologyEvent, TopologyListener, TopologyService}
import org.osgi.service.component.annotations.Activate
import org.apache.felix.scr.annotations.Component
import org.osgi.service.component.annotations.Deactivate
import org.slf4j.LoggerFactory
import org.apache.felix.scr.annotations.Reference
import org.apache.felix.scr.annotations.ReferenceCardinality
import org.onosproject.cfg.ComponentConfigService
import org.onosproject.net.device.DeviceService
import org.onosproject.net.flow.criteria.{Criterion, EthCriterion}
import org.onosproject.net.link.LinkEvent
import org.osgi.service.component.ComponentContext

import scala.collection.{JavaConverters, mutable}


/**
  * Skeletal ONOS application component.
  */
@Component(immediate = true)
class AppComponent {
    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY) var topologyService: TopologyService = _
    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY) var cfgService: ComponentConfigService = _
    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY) var coreService: CoreService = _
    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY) var flowRuleService: FlowRuleService = _
    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY) var packetService: PacketService = _
    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY) var hostService: HostService = _
    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY) var flowObjectiveService: FlowObjectiveService = _
    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY) var deviceService: DeviceService = _
    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY) var pathService: PathService = _
    var appId: ApplicationId = _
    val topologyListener = new MyTopologyListener
    var packetProcessor = new MyPacketProcessor

    final private val log = LoggerFactory.getLogger(getClass)

    @Activate def activate(context: ComponentContext): Unit = {
        cfgService.registerProperties(getClass)
        appId = coreService.registerApplication("test scala")
        packetService.addProcessor(packetProcessor, PacketProcessor.director(2))
        topologyService.addListener(topologyListener)
        requestIntercepts()
        log.info("scala Started")
    }

    @Deactivate def deactivate(): Unit = {
        cfgService.unregisterProperties(getClass, false)
        withdrawIntercepts()
        flowRuleService.removeFlowRulesById(appId)
        packetService.removeProcessor(packetProcessor)
        topologyService.removeListener(topologyListener)
        packetProcessor = null
        log.info("Stopped")
    }

    def requestIntercepts(): Unit = {
        val selector = DefaultTrafficSelector.builder()
        selector.matchEthType(Ethernet.TYPE_IPV4)
        packetService.requestPackets(selector.build(), PacketPriority.REACTIVE, appId)
    }

    def withdrawIntercepts(): Unit = {
        val selector = DefaultTrafficSelector.builder()
        selector.matchEthType(Ethernet.TYPE_IPV4)
        packetService.cancelPackets(selector.build(), PacketPriority.REACTIVE, appId)
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
            val srcPaths = scala.collection.mutable.HashMap.empty[DeviceId,mutable.Set[Path]]
            pairs.foreach(p=>{
                val srcHost = hostService.getHost(HostId.hostId(p._1))
                val dstHost = hostService.getHost(HostId.hostId(p._2))
                if(srcHost!=null&&dstHost!=null) {
                    val srcId = srcHost.location().deviceId()
                    val dstId = dstHost.location().deviceId()
                    var shortestPaths=srcPaths.get(dstId)
                    if(shortestPaths.isEmpty){
                        shortestPaths = Some(JavaConverters.asScalaSet(topologyService.getPaths(topologyService.currentTopology(),egress.deviceId(),dstId)))
                        srcPaths.put(dstId,shortestPaths.get)
                    }
                    backTrackBadNodes(shortestPaths.get,srcId,p)
                }
            })
        }

        // 1 -> 2 -> 3
        def backTrackBadNodes(shortestPaths:mutable.Set[Path],dstId:DeviceId,sd:(MacAddress,MacAddress)): Unit = {
            shortestPaths.foreach(p=>{
                val pathLinks = p.links()
                breakable({
                    for(i<-0 until pathLinks.size()) {
                        val curLink = pathLinks.get(i)
                        val curDevice = curLink.src().deviceId()

                            // clean
                        cleanFlowRules(sd,curDevice)

                        val pathsFromCurDevice = topologyService.getPaths(topologyService.currentTopology(),curDevice,dstId)
                        if(pickForwardPathIfPossible(pathsFromCurDevice,curLink.src().port()).isDefined){
                            break
                        }
                        else {
                            if(i+1 == pathLinks.size()) {
                                cleanFlowRules(sd,curLink.dst().deviceId())
                            }
                        }
                    }
                })
            })
        }

        def cleanFlowRules(pair: (MacAddress,MacAddress), id: DeviceId): Unit = {
            flowRuleService.getFlowEntries(id).forEach(r=>{
                var matchesSrc = false
                var matchesDst = false
                r.treatment().allInstructions().forEach(i=>{
                    if (i.`type`.equals(Instruction.Type.OUTPUT)) { // if the flow has matching src and dst
                        r.selector().criteria().forEach(cr=>{
                            if(cr.`type`().equals(Criterion.Type.ETH_DST)){
                                if(cr.asInstanceOf[EthCriterion].mac().equals(pair._2)) {
                                    matchesDst = true
                                }
                            }
                            else if (cr.`type`().equals(Criterion.Type.ETH_SRC)){
                                if(cr.asInstanceOf[EthCriterion].mac().equals(pair._1)) {
                                    matchesSrc = true
                                }
                            }
                        })
                    }
                })
                if (matchesDst && matchesSrc) {
                    log.info("Removed flow rule from device == {}",id)
                    flowRuleService.removeFlowRules(r.asInstanceOf[FlowRule])
                }
            })
        }

        def pickForwardPathIfPossible(paths: util.Set[Path], number: PortNumber):Option[Path] = {
            var lastPath:Option[Path] = None
            val t =JavaConverters.asScalaSet(paths).flatMap(p=>{
                lastPath=Some(p)
                if(!p.src().port().equals(number)) {
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
                            case _=>None
                        })
                })
                .toList
        }

        def findSrcDstPairs(rules:List[FlowEntry]): List[(MacAddress, MacAddress)] = {
            rules.map(r=>{
                var src:MacAddress = null
                var dst:MacAddress = null
                r.selector().criteria().forEach(cr=>{
                    if(cr.`type`()==Criterion.Type.ETH_DST) {
                        dst=cr.asInstanceOf[EthCriterion].mac()
                    }
                    else if(cr.`type`()==Criterion.Type.ETH_SRC) {
                        src=cr.asInstanceOf[EthCriterion].mac()
                    }
                })
                (src,dst)
            })
        }
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
            if (context.isHandled) {
                return
            }

            val inPacket = context.inPacket()
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
                if(topologyService.isBroadcastPoint(topologyService.currentTopology(),context.inPacket().receivedFrom())){
                    packetOut(context,PortNumber.FLOOD)
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
            if(dstIp.isLinkLocal||dstIp.isMulticast||dstIp.isZero||dstIp==Ip4Address.valueOf("192.168.1.255")||dstIp==Ip4Address.valueOf("255.255.255.255")){
                flood(context)
                return
            }
            val dsthosts = hostService.getHostsByIp(dstIp)
            if (dsthosts.size() != 1) {
                log.info("dstIp == {}",dstIp.toString)
                log.info("dstHosts size == {}",dsthosts.size())
                log.info("flood")
                flood(context)
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
                    // flood(context)
                    case Some(nexthop) =>
                        //                        log.info("nexthop device id == {}", nexthop._1.toString)
                        //                        log.info("nexthop out port == {}", nexthop._2.toString)
                        //                        log.info("nexthop out port is logical == {}",nexthop._2.isLogical.toString)
                        installInRule(srcIp, ethPacket.getSourceMAC, dstIp, ethPacket.getDestinationMAC, inPacket.receivedFrom().port(), curDeviceId, curMac, toMacAddress(nexthop._1))
                        installOutRule(srcIp, ethPacket.getSourceMAC, dstIp, ethPacket.getDestinationMAC, nexthop._2, curDeviceId, curMac, toMacAddress(nexthop._1), context)
                }
            }
            else if (curDeviceId == dstDeviceId) {
                log.info("at dest edge node == {}", dstDeviceId.toString)
                val hostOutport = findHostOutport(curDeviceId, dstHost.id()).get
                // install out rule
                val srcMac = ethPacket.getSourceMAC
                installOutRule(srcIp, srcMac, dstIp, curMac, hostOutport, curDeviceId, srcHost.mac(), dstHost.mac(), context)
                //                val selectorBuilder = DefaultTrafficSelector.builder()
                //                val ipv4srcMatch = Ip4Prefix.valueOf(srcIp, Ip4Prefix.MAX_MASK_LENGTH)
                //                val ipv4dstMatch = Ip4Prefix.valueOf(dstIp, Ip4Prefix.MAX_MASK_LENGTH)
                //                selectorBuilder.matchEthDst(curMac)
                //                    .matchEthSrc(srcMac)
                //                    .matchIPSrc(ipv4srcMatch)
                //                    .matchIPDst(ipv4dstMatch)
                //                    .matchEthType(Ethernet.TYPE_IPV4)
                //
                //                val treatment = DefaultTrafficTreatment.builder()
                //                    .setEthDst(dstHost.mac())
                //                    .setEthSrc(srcHost.mac())
                //                    .setOutput(hostOutport)
                //                    .build()
                //
                //                val forwardingObjective = DefaultForwardingObjective.builder()
                //                    .withSelector(selectorBuilder.build())
                //                    .withTreatment(treatment)
                //                    .withPriority(10)
                //                    .fromApp(appId)
                //                    .withFlag(ForwardingObjective.Flag.VERSATILE)
                //                    .makeTemporary(2000)
                //                    .add();
                //                flowObjectiveService.forward(curDeviceId, forwardingObjective);
                //                val packet = context.inPacket().parsed()
                //                packet.setDestinationMACAddress(dstHost.mac())
                //                packet.setSourceMACAddress(srcHost.mac())
                //                packetService.emit(new DefaultOutboundPacket(curDeviceId,treatment,ByteBuffer.wrap(packet.serialize())));
                // install in rule
                installInRule(srcIp, srcMac, dstIp, curMac, context.inPacket().receivedFrom().port(), curDeviceId, srcHost.mac(), dstHost.mac())
                //                {
                //                    val selectorBuilder = DefaultTrafficSelector.builder()
                //                    val ipv4srcMatch = Ip4Prefix.valueOf(srcIp, Ip4Prefix.MAX_MASK_LENGTH)
                //                    val ipv4dstMatch = Ip4Prefix.valueOf(dstIp, Ip4Prefix.MAX_MASK_LENGTH)
                //                    selectorBuilder.matchEthDst(srcHost.mac())
                //                        .matchEthSrc(dstHost.mac())
                //                        .matchIPSrc(ipv4dstMatch)
                //                        .matchIPDst(ipv4srcMatch)
                //                        .matchEthType(Ethernet.TYPE_IPV4)
                //
                //                    val treatment = DefaultTrafficTreatment.builder()
                //                        .setEthDst(srcMac)
                //                        .setEthSrc(curMac)
                //                        .setOutput(context.inPacket().receivedFrom().port())
                //                        .build()
                //
                //                    val forwardingObjective = DefaultForwardingObjective.builder()
                //                        .withSelector(selectorBuilder.build())
                //                        .withTreatment(treatment)
                //                        .withPriority(10)
                //                        .fromApp(appId)
                //                        .withFlag(ForwardingObjective.Flag.VERSATILE)
                //                        .makeTemporary(2000)
                //                        .add()
                //
                //                    flowObjectiveService.forward(curDeviceId, forwardingObjective);
                //                }
            }
            else {
                log.info("at inter node")
                log.info("src == {}", srcDeviceId.toString)
                log.info("dst == {}", dstDeviceId.toString)
                log.info("cur == {}", curDeviceId.toString)
                findNextHopDevice(curDeviceId, srcDeviceId, dstDeviceId) match {
                    case None =>
                        log.info("find none hop")
                    // flood(context)
                    case Some(nexthop) =>
                        val inPort = context.inPacket().receivedFrom().port()
                        log.info("inPort == {}", inPort.toString)
                        //                        log.info("nexthop device id == {}", nexthop._1.toString)
                        //                        log.info("nexthop out port == {}", nexthop._2.toString)
                        //                        log.info("nexthop out port is logical == {}",nexthop._2.isLogical.toString)
                        //                        log.info("srcIp == {}",srcIp.toString)
                        //                        log.info("srcMac == {}",ethPacket.getSourceMAC.toString)
                        //                        log.info("dstIp == {}",dstIp.toString)
                        //                        log.info("dstMac == {}",ethPacket.getDestinationMAC.toString)
                        //                        log.info("curMac == {}",curMac.toString)
                        //                        log.info("nextMac == {}",toMacAddress(nexthop._1))
                        // install out rule
                        val srcMac = ethPacket.getSourceMAC
                        installOutRule(srcIp, srcMac, dstIp, curMac, PortNumber.IN_PORT, curDeviceId, curMac, toMacAddress(nexthop._1), context)
                        //                        val selectorBuilder = DefaultTrafficSelector.builder()
                        //                        val ipv4srcMatch = Ip4Prefix.valueOf(srcIp, Ip4Prefix.MAX_MASK_LENGTH)
                        //                        val ipv4dstMatch = Ip4Prefix.valueOf(dstIp, Ip4Prefix.MAX_MASK_LENGTH)
                        //                        selectorBuilder.matchEthDst(curMac)
                        //                            .matchEthSrc(srcMac)
                        //                            .matchIPSrc(ipv4srcMatch)
                        //                            .matchIPDst(ipv4dstMatch)
                        //                            .matchEthType(Ethernet.TYPE_IPV4)
                        //
                        //                        val treatment = DefaultTrafficTreatment.builder()
                        //                            .setEthDst(toMacAddress(nexthop._1))
                        //                            .setEthSrc(curMac)
                        //                            .setOutput(nexthop._2)
                        //                            .build()
                        //
                        //                        val forwardingObjective = DefaultForwardingObjective.builder()
                        //                            .withSelector(selectorBuilder.build())
                        //                            .withTreatment(treatment)
                        //                            .withPriority(10)
                        //                            .fromApp(appId)
                        //                            .withFlag(ForwardingObjective.Flag.VERSATILE)
                        //                            .makeTemporary(2000)
                        //                            .add()
                        //
                        //                        val packet = context.inPacket().parsed()
                        //                        packet.setDestinationMACAddress(toMacAddress(nexthop._1))
                        //                        packet.setSourceMACAddress(curMac)
                        //                        packetService.emit(new DefaultOutboundPacket(curDeviceId,treatment,ByteBuffer.wrap(packet.serialize())))
                        // install in rule
                        installInRule(srcIp, srcMac, dstIp, curMac, PortNumber.IN_PORT, curDeviceId, curMac, toMacAddress(nexthop._1))
                    //                        {
                    //                            var selectorBuilder = DefaultTrafficSelector.builder()
                    //                            val ipv4srcMatch = Ip4Prefix.valueOf(srcIp, Ip4Prefix.MAX_MASK_LENGTH)
                    //                            val ipv4dstMatch = Ip4Prefix.valueOf(dstIp, Ip4Prefix.MAX_MASK_LENGTH)
                    //                            selectorBuilder.matchEthDst(curMac)
                    //                                .matchEthSrc(toMacAddress(nexthop._1))
                    //                                .matchIPSrc(ipv4dstMatch)
                    //                                .matchIPDst(ipv4srcMatch)
                    //                                .matchEthType(Ethernet.TYPE_IPV4)
                    //
                    //                            val treatment = DefaultTrafficTreatment.builder()
                    //                                .setEthDst(srcMac)
                    //                                .setEthSrc(curMac)
                    //                                .setOutput(inPort)
                    //                                .build()
                    //
                    //                            val forwardingObjective = DefaultForwardingObjective.builder()
                    //                                .withSelector(selectorBuilder.build())
                    //                                .withTreatment(treatment)
                    //                                .withPriority(10)
                    //                                .fromApp(appId)
                    //                                .withFlag(ForwardingObjective.Flag.VERSATILE)
                    //                                .makeTemporary(2000)
                    //                                .add()
                    //
                    //                            flowObjectiveService.forward(curDeviceId, forwardingObjective)
                    //                        }
                }
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
//
//        def checkConnectivity(src:DeviceId,dst:DeviceId):Boolean = {
//
//        }

        def findHostOutport(currentDeviceId: DeviceId, hostId: HostId): Option[PortNumber] = {
            val paths = pathService.getPaths(currentDeviceId, hostId)
            val p: mutable.Set[PortNumber] = JavaConverters.asScalaSet(paths).map(_.links().get(0))
                .map(l => l.src().port())
            p.headOption
        }

        def installInRule(srcIp: Ip4Address, srcMac: MacAddress, dstIp: Ip4Address, dstMac: MacAddress, inPort: PortNumber, curDeviceId: DeviceId, curMac: MacAddress, nextHopMac: MacAddress): Unit = {
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

            val forwardingObjective = DefaultForwardingObjective.builder()
                .withSelector(selectorBuilder.build())
                .withTreatment(treatment)
                .withPriority(10)
                .fromApp(appId)
                .withFlag(ForwardingObjective.Flag.VERSATILE)
                .makeTemporary(2000)
                .add()
            log.info("install in rule")
            flowObjectiveService.forward(curDeviceId, forwardingObjective)
        }

        def installOutRule(srcIp: Ip4Address, srcMac: MacAddress, dstIp: Ip4Address, dstMac: MacAddress, outPort: PortNumber, curDeviceId: DeviceId, curMac: MacAddress, nextHopMac: MacAddress, context: PacketContext): Unit = {
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

            val forwardingObjective = DefaultForwardingObjective.builder()
                .withSelector(selectorBuilder.build())
                .withTreatment(treatment)
                .withPriority(10)
                .fromApp(appId)
                .withFlag(ForwardingObjective.Flag.VERSATILE)
                .makeTemporary(2000)
                .add()
            log.info("install out rule")
            flowObjectiveService.forward(curDeviceId, forwardingObjective)

            val packet = context.inPacket().parsed()
            packet.setDestinationMACAddress(nextHopMac)
            packet.setSourceMACAddress(curMac)
            packetService.emit(new DefaultOutboundPacket(curDeviceId, treatment, ByteBuffer.wrap(packet.serialize())))
        }

        def isControlPacket(ethPkt: Ethernet): Boolean = {
            ethPkt.getEtherType == Ethernet.TYPE_LLDP || ethPkt.getEtherType == Ethernet.TYPE_BSN
        }

        def flood(context: PacketContext): Unit = {
            if (topologyService.isBroadcastPoint(topologyService.currentTopology(), context.inPacket().receivedFrom())) {
                packetOut(context, PortNumber.FLOOD)
            }
            else {
                log.info("block at device == {}",context.inPacket().receivedFrom().deviceId())
                log.info("block at port == {}",context.inPacket().receivedFrom().port())
                context.block()
            }
        }

        def packetOut(context: PacketContext): Unit = {
            context.send()
        }

        def packetOut(context: PacketContext, number: PortNumber): Unit = {
            context.treatmentBuilder().setOutput(number)
            context.send()
        }

        def handleARPResponse(arp: ARP, context: PacketContext): Unit = {
            val dstIp = IpAddress.valueOf(IpAddress.Version.INET, arp.getTargetProtocolAddress)
            val dstMac = MacAddress.valueOf(arp.getTargetHardwareAddress)
            val srcIp = IpAddress.valueOf(IpAddress.Version.INET, arp.getSenderProtocolAddress)
            val srcDst = MacAddress.valueOf(arp.getSenderHardwareAddress)
            val deviceId = context.inPacket().receivedFrom().deviceId()
            log.info("arp response: dstIp == {}", dstIp.toString)
            log.info("arp response: dstMac == {}", dstMac.toString)
            log.info("arp response: srcIp == {}", srcIp.toString)
            log.info("arp response: srcMac == {}", srcDst.toString)
            log.info("arp response: deviceId == {}", deviceId.toString)
            flood(context)
        }
    }

}