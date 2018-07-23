package py.infocenter.store;

import py.icshare.ServerNode;

import java.util.List;
import java.util.Set;

public interface ServerNodeStore {
    void clearDB();

    void saveOrUpdateServerNode(ServerNode serverNode);

    void deleteServerNodes(List<String> serverNodeIds);

    void updateServerNode(ServerNode serverNode);

    List<ServerNode> listAllServerNodes();

    ServerNode getServerNodeByIp(String ip);

    ServerNode listServerNodeById(String id);

    List<ServerNode> listServerNodes(int offset, int limit, String sortField, String sortDirection, String hostName, String modelInfo,
            String cpuInfo, String memoryInfo, String diskInfo, String networkCardInfo, String manageIp,
            String gatewayIp, String storeIp, String rackNo, String slotNo);

    int getCountTotle();
}
