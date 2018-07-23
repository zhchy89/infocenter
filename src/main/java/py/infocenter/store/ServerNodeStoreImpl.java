package py.infocenter.store;

import org.h2.tools.Server;
import org.hibernate.Criteria;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.MatchMode;
import org.hibernate.criterion.Restrictions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;
import py.icshare.ServerNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

@Transactional public class ServerNodeStoreImpl implements ServerNodeStore {
    private static final Logger logger = LoggerFactory.getLogger(ServerNodeStoreImpl.class);
    private SessionFactory sessionFactory;

    public void setSessionFactory(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    @Override public void clearDB() {
//        Query query = sessionFactory.getCurrentSession().createQuery("delete ServerNode where 1=1 ");
//        query.executeUpdate();
        List<ServerNode> serverNodeList = listAllServerNodes();
        List<String> ids = new ArrayList<>();
        for (ServerNode serverNode1 : serverNodeList) {
            ids.add(serverNode1.getId());
        }
        deleteServerNodes(ids);
    }

    @Override public void saveOrUpdateServerNode(ServerNode serverNode) {
        sessionFactory.getCurrentSession().saveOrUpdate(serverNode);
    }

    @Override public void deleteServerNodes(List<String> serverNodeIds) {
        for (String serverNodeId : serverNodeIds) {
            deleteServerNodeById(serverNodeId);
        }
    }

    private void deleteServerNodeById(String serverNodeId) {
        if (serverNodeId == null) {
            logger.error("Invalid parameter, serverNodeId:{}", serverNodeId);
            return;
        }
        try {
            ServerNode serverNode = sessionFactory.getCurrentSession().get(ServerNode.class, serverNodeId);
            sessionFactory.getCurrentSession().delete(serverNode);
        } catch (Exception e) {
            logger.error("caught an exception: {}", e);
            throw e;
        }
    }

    @Override public void updateServerNode(ServerNode serverNode) {
        sessionFactory.getCurrentSession().update(serverNode);
    }

    @Override
    public List<ServerNode> listAllServerNodes(){
        List<ServerNode> serverNodeList = sessionFactory.getCurrentSession().createQuery("from ServerNode").list();
        return serverNodeList;
    }

    @SuppressWarnings("deprecation")
    @Override
    public ServerNode getServerNodeByIp(String ip){
        Session currentSession = sessionFactory.getCurrentSession();
        Criteria criteria = currentSession.createCriteria(ServerNode.class);
        criteria.add(Restrictions.ilike("networkCardInfoName", ip, MatchMode.ANYWHERE));
        List list = criteria.list();
        if (list.size() != 0) {
            return (ServerNode) list.get(0);
        } else {
            return null;
        }
    }

    @Override
    public ServerNode listServerNodeById(String id){
        ServerNode serverNode = sessionFactory.getCurrentSession().get(ServerNode.class, id);
        return serverNode;
    }

    /**
     * @param offset
     * @param limit
     * @param sortField
     * @param sortDirection   "ASC" | "DESC"
     * @param modelInfo
     * @param cpuInfo
     * @param memoryInfo
     * @param diskInfo
     * @param networkCardInfo
     * @param manageIp
     * @param gatewayIp
     * @param storeIp
     * @param rackNo
     * @param slotNo
     * @return
     */
    @Override public List<ServerNode> listServerNodes(int offset, int limit, String sortField, String sortDirection,
            String hostName, String modelInfo, String cpuInfo, String memoryInfo, String diskInfo, String networkCardInfo,
            String manageIp, String gatewayIp, String storeIp, String rackNo, String slotNo) {
        logger.debug(
                "listServerNodes parameter: limit-{}, page-{}, sortField-{}, sortDirection-{},modelInfo-{}, cpuInfo-{}, memoryInfo-{}, diskInfo-{}, networkCardInfo-{}, manageIp-{}, gatewayIp-{}, storeIp-{}, rackNo-{}, slotNo-{}",
                offset, limit, sortField, sortDirection, modelInfo, cpuInfo, memoryInfo, diskInfo, networkCardInfo,
                manageIp, gatewayIp, storeIp, rackNo, slotNo);

        StringBuilder hqlSequenceSB = new StringBuilder("from ServerNode ");
        String hqlFilterStr = getFilterStr(hostName, modelInfo, cpuInfo, memoryInfo, diskInfo, networkCardInfo, manageIp,
                gatewayIp, storeIp, rackNo, slotNo);
        hqlSequenceSB.append(hqlFilterStr);

        if (sortField == null) {
            sortField = "networkCardInfoName";
        }
        if (!Objects.equals(sortDirection, "ASC") && !Objects.equals(sortDirection, "DESC")) {
            sortDirection = "ASC";
        }
        hqlSequenceSB.append(String.format(" order by %s %s ", sortField, sortDirection));

        String hqlSequenceStr = hqlSequenceSB.toString();
        try {
            Query query = sessionFactory.getCurrentSession().createQuery(hqlSequenceStr);
            query.setFirstResult(offset);
            query.setMaxResults(limit);
            logger.debug("hqlSequenceStr: {}", hqlSequenceStr);
            List<ServerNode> serverNodeList = query.list();
            return serverNodeList;
        } catch (Exception e) {
            logger.error("caught an exception", e);
            throw e;
        }
    }

    @Override
    public int getCountTotle(){
        String hqlString = "select count(id) from ServerNode ";
        Query query = sessionFactory.getCurrentSession()
                .createQuery(hqlString);
        return ((Number)query.uniqueResult()).intValue();
    }

    private String getFilterStr(String hostName, String modelInfo, String cpuInfo, String memoryInfo, String diskInfo,
            String networkCardInfo, String manageIp, String gatewayIp, String storeIp, String rackNo, String slotNo) {

        StringBuilder sqlSequenceSB = new StringBuilder(String.format(" where 1 = 1"));

        if (manageIp != null) {
            sqlSequenceSB.append(String.format("and lower(manageIp) = lower('%s') ", manageIp));
        }
        if (rackNo != null) {
            sqlSequenceSB.append(String.format("and lower(rackNo) = lower('%s') ", rackNo));
        }
        if (slotNo != null) {
            sqlSequenceSB.append(String.format("and lower(slotNo) = lower('%s') ", slotNo));
        }
        if (hostName != null) {
            sqlSequenceSB.append(String.format("and lower(hostName) like lower('%%"+ "%s" + "%%') ", hostName));
        }
        if (modelInfo != null) {
            sqlSequenceSB.append(String.format("and lower(modelInfo) like lower('%%"+ "%s" + "%%') ", modelInfo));
        }
        if (cpuInfo != null) {
            sqlSequenceSB.append(String.format("and lower(cpuInfo) like lower('%%"+ "%s" + "%%') ", cpuInfo));
        }
        if (memoryInfo != null) {
            sqlSequenceSB.append(String.format("and lower(memoryInfo) like lower('%%"+ "%s" + "%%') ", memoryInfo));
        }
        if (diskInfo != null) {
            sqlSequenceSB.append(String.format("and lower(diskInfo) like lower('%%"+ "%s" + "%%') ", diskInfo));
        }
        if (networkCardInfo != null) {
            sqlSequenceSB.append(String.format("and lower(networkCardInfo) like lower('%%"+ "%s" + "%%') ", networkCardInfo));
        }
        if (storeIp != null) {
            sqlSequenceSB.append(String.format("and lower(storeIp) like lower('%%"+ "%s" + "%%') ", storeIp));
        }
        if (gatewayIp != null) {
            sqlSequenceSB.append(String.format("and lower(gatewayIp) like lower('%%"+ "%s" + "%%') ", gatewayIp));
        }

        return sqlSequenceSB.toString();
    }

}
