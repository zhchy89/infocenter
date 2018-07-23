package py.infocenter.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.PyService;
import py.icshare.InstanceMaintenanceDBStore;
import py.icshare.InstanceMaintenanceInformation;
import py.icshare.ServerNode;
import py.infocenter.store.ServerNodeStore;
import py.instance.Instance;
import py.instance.InstanceStatus;
import py.instance.InstanceStore;
import py.monitorserver.common.CounterName;
import py.monitorserver.common.OperationName;
import py.monitorserver.common.UserDefineName;
import py.periodic.Worker;
import py.querylog.EventDataUtil.EventDataWorker;

import java.util.*;

public class ServerNodeAlertChecker implements Worker {
    private static final Logger logger = LoggerFactory.getLogger(ServerNodeAlertChecker.class);

    private Map<String, Long> serverNodeReportTimeMap;
    private int serverNodeReportOverTimeSecond;
    private InstanceStore instanceStore;
    private ServerNodeStore serverNodeStore;
    private InstanceMaintenanceDBStore instanceMaintenanceDBStore;

    public ServerNodeAlertChecker(Map<String, Long> serverNodeReportTimeMap, int serverNodeReportOverTimeSecond,
            InstanceStore instanceStore, ServerNodeStore serverNodeStore, InstanceMaintenanceDBStore instanceMaintenanceDBStore) {
        this.serverNodeReportTimeMap = serverNodeReportTimeMap;
        this.serverNodeReportOverTimeSecond = serverNodeReportOverTimeSecond;
        this.instanceStore = instanceStore;
        this.serverNodeStore = serverNodeStore;
        this.instanceMaintenanceDBStore = instanceMaintenanceDBStore;
    }

    @Override
    public void doWork() throws Exception {
        logger.debug("begin serverNodeAlertChecker, serverNodeReportTimeMap: {}", serverNodeReportTimeMap);
        List<String> list = new ArrayList<>();

        Set<Instance> dIHInstancesTemp = instanceStore.getAll(PyService.DIH.getServiceName(), InstanceStatus.OK);

        for (Instance instance : dIHInstancesTemp) {

            ServerNode serverNodeFromStore = serverNodeStore.getServerNodeByIp(instance.getEndPoint().getHostName());

            if (serverNodeFromStore != null) {
                serverNodeReportTimeMap.put(serverNodeFromStore.getId(), System.currentTimeMillis());
                if (!serverNodeFromStore.getStatus().contains("ok")) {
                    serverNodeFromStore.setStatus("ok");
                    serverNodeStore.updateServerNode(serverNodeFromStore);
                }
            }
        }


        for (Map.Entry<String, Long> entry : serverNodeReportTimeMap.entrySet()) {
            String serverId = entry.getKey();
            Long lastReportTime = entry.getValue();
            ServerNode serverNode = serverNodeStore.listServerNodeById(serverId);
            if (serverNode == null) {
                logger.debug("new server node joined, serverNodeId: {}", serverId);
                continue;
            }
            String serverNodeIp = serverNode.getNetworkCardInfoName();
            String serverNodeHostName = serverNode.getHostName();

            long currentTime = System.currentTimeMillis();
            logger.debug("lastReportTime: {}, currentTime: {}, currentTime-lastReportTime: {}", lastReportTime,
                    currentTime, currentTime - lastReportTime);
            if (currentTime - lastReportTime > serverNodeReportOverTimeSecond * 1000) {

                Set<Instance> DIHInstances = instanceStore.getAll(PyService.DIH.getServiceName(), InstanceStatus.OK);

                boolean flag = false;
                for (Instance instance : DIHInstances) {
                    if (serverNodeIp.contains(instance.getEndPoint().getHostName())) {
                        flag = true;
                        break;
                    }
                }

                if (flag) {
                    break;
                }

                Set<Instance> systemDaemonInstances = instanceStore.getAll(PyService.SYSTEMDAEMON.getServiceName());
                for (Instance instance : systemDaemonInstances) {
                    if (serverNodeIp.contains(instance.getEndPoint().getHostName())) {
                        logger.warn("before modify, alert server node ip is: {}", instance.getEndPoint().getHostName());
                        serverNodeIp = instance.getEndPoint().getHostName();
                        logger.warn("after modify, alert server node ip is: {}", instance.getEndPoint().getHostName());
                        break;
                    }
                }

                Map<String, String> defaultNameValues = new HashMap<>();
                defaultNameValues.put(UserDefineName.ServerNodeIp.toString(), serverNodeIp);
                defaultNameValues.put(UserDefineName.ServerNodeHostName.toString(), serverNodeHostName);
                defaultNameValues.put(UserDefineName.ServerNodeRackNo.toString(), serverNode.getRackNo());
                defaultNameValues.put(UserDefineName.ServerNodeChildFramNo.toString(), serverNode.getChildFramNo());
                defaultNameValues.put(UserDefineName.ServerNodeSlotNo.toString(), serverNode.getSlotNo());
                EventDataWorker eventDataWorker = new EventDataWorker(PyService.INFOCENTER, defaultNameValues);

                Boolean generatedEventData = true;
                List<InstanceMaintenanceInformation> instanceMaintenanceInformationList = instanceMaintenanceDBStore.listAll();
                for (InstanceMaintenanceInformation instanceMaintenanceInformation: instanceMaintenanceInformationList) {

                    if (System.currentTimeMillis() > instanceMaintenanceInformation.getEndTime()) {
                        instanceMaintenanceDBStore.delete(instanceMaintenanceInformation);
                        continue;
                    }

                    Long instanceId = instanceMaintenanceInformation.getInstanceId();
                    Set<Instance> instanceSet = instanceStore.getAll();

                    String  serverNodeIpString;

                    for (Instance instance : instanceSet) {
                        if (instance.getId().getId() == instanceId) {
                            serverNodeIpString = instanceMaintenanceDBStore.getById(instanceId).getIp();
                            if (serverNodeIp.equals(serverNodeIpString)) {
                                generatedEventData = false;
                                break;
                            }
                        }
                    }
                }

                if (generatedEventData) {
                    eventDataWorker
                            .work(OperationName.ServerNode.toString(), CounterName.SERVERNODE_STATUS.toString(), 0, 0);
                }
                serverNode.setStatus("unknown");
                serverNodeStore.updateServerNode(serverNode);

                logger.warn("serverNode report timeout, serverNodeIp: {}", serverNodeIp);

                //                serverNodeReportTimeMap.put(serverId, currentTime);
                list.add(serverId);
            }
        }

        for (String serverId : list) {
            serverNodeReportTimeMap.remove(serverId);
        }
    }

}
