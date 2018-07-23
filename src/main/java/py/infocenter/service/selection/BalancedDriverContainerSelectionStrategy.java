package py.infocenter.service.selection;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import py.driver.DriverContainerCandidate;
import py.driver.DriverMetadata;
import py.instance.Instance;
import py.instance.PortType;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * select driver container to launcher a driver
 * 
 * selection strategy based on driver container balance
 * 
 * in case the selection driver container died, every time return three candidate for controlcenter if there are enough
 * instance
 * 
 * @author liy
 * 
 */
public class BalancedDriverContainerSelectionStrategy implements DriverContainerSelectionStrategy {
    private static final Logger logger = LoggerFactory.getLogger(BalancedDriverContainerSelectionStrategy.class);

    @Override
    public List<DriverContainerCandidate> select(Collection<Instance> instances, List<DriverMetadata> drivers) {
        logger.debug("Going to balance sort instances {} base on existing drivers {}", instances, drivers);

        List<DriverContainerCandidate> driverContainerCandidates = new ArrayList<DriverContainerCandidate>();

        if (instances == null || instances.size() == 0) {
            return driverContainerCandidates;
        }

        TreeMap<String, DriverContainerBalance> driverBalanceMaps = new TreeMap<String, DriverContainerBalance>();
        if (drivers == null) {
            drivers = new ArrayList<DriverMetadata>();
        }
        // list all current driver container balance
        for (DriverMetadata driverMetadata : drivers) {
            DriverContainerBalance driverContainerBalance = driverBalanceMaps.get(driverMetadata.getHostName());
            if (driverContainerBalance == null) {
                driverContainerBalance = new DriverContainerBalance();
                driverContainerBalance.setHostname(driverMetadata.getHostName());
                driverContainerBalance.setCount(1);
                driverBalanceMaps.put(driverContainerBalance.getHostname(), driverContainerBalance);
            } else {
                driverContainerBalance.setCount(driverContainerBalance.getCount() + 1);
            }
        }
        List<DriverContainerBalance> driverBalanceList = new ArrayList<DriverContainerBalance>(
                driverBalanceMaps.values());

        Map<String, Instance> toSelectInstance = new ConcurrentHashMap<String, Instance>();
        for (Instance instance : instances) {
            toSelectInstance.put(instance.getEndPointByServiceName(PortType.IO).getHostName(), instance);
        }

        for (Instance instance : instances) {
            DriverContainerBalance driverContainerBalance = driverBalanceMaps
                    .get(instance.getEndPointByServiceName(PortType.IO).getHostName());
            if (driverContainerBalance == null) {
                DriverContainerBalance tmpDriverContainerBalance = new DriverContainerBalance();
                tmpDriverContainerBalance.setCount(0);
                tmpDriverContainerBalance
                        .setHostname(instance.getEndPointByServiceName(PortType.IO).getHostName());
                driverBalanceList.add(tmpDriverContainerBalance);
            }
        }
        // sort driver container balance
        Collections.sort(driverBalanceList, new DriverBalanceComparator());

        // pick instance as candidate
        for (DriverContainerBalance driverContainerBalance : driverBalanceList) {
            Instance instance = toSelectInstance.get(driverContainerBalance.getHostname());
            if (instance != null) {
                DriverContainerCandidate driverContainerCandidate = new DriverContainerCandidate();
                driverContainerCandidate.setHostName(instance.getEndPointByServiceName(PortType.CONTROL).getHostName());
                driverContainerCandidate.setPort(instance.getEndPointByServiceName(PortType.CONTROL).getPort());
                driverContainerCandidates.add(driverContainerCandidate);
            }
        }
        return driverContainerCandidates;
    }

    class DriverContainerBalance {

        private String hostname;

        private int count;

        public String getHostname() {
            return hostname;
        }

        public void setHostname(String hostname) {
            this.hostname = hostname;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }
    }

    private class DriverBalanceComparator implements Comparator<DriverContainerBalance> {

        @Override
        public int compare(DriverContainerBalance o1, DriverContainerBalance o2) {

            return o1.getCount() - o2.getCount();
        }

    }

}
