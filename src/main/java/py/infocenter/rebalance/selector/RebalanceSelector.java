package py.infocenter.rebalance.selector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.struct.Pair;
import py.infocenter.rebalance.InstanceInfo;
import py.infocenter.rebalance.QuantifiableSelector;
import py.infocenter.rebalance.RebalanceConfiguration;
import py.infocenter.rebalance.exception.NoNeedToRebalance;
import py.infocenter.rebalance.exception.NoSuitableTask;
import py.infocenter.rebalance.struct.ComparableRebalanceTask;
import py.infocenter.rebalance.struct.InstanceInfoImpl;
import py.infocenter.rebalance.struct.SimpleRebalanceTask;
import py.rebalance.RebalanceTask;

import java.util.*;

/**
 * @author tyr
 */
@Deprecated
public class RebalanceSelector {
    private static final Logger logger = LoggerFactory.getLogger(RebalanceSelector.class);
    private final RebalanceConfiguration config = RebalanceConfiguration.getInstance();
    private final LinkedList<InstanceInfoImpl> instanceInfoList;

    private static final QuantifiableSelector<InstanceInfoImpl> PRIMARY_SELECTOR = new QuantifiableSelector<>(
            value -> (int) value.primaryPressure() * 100);
    private static final QuantifiableSelector<InstanceInfoImpl> NORMAL_SELECTOR = new QuantifiableSelector<>(
            value -> (int) value.calculatePressure() * 100);
    private static final Map<RebalanceTask.RebalanceTaskType, QuantifiableSelector<InstanceInfoImpl>> MAP_TASK_TYPE_TO_SELECTOR = new HashMap<>();

    static {
        MAP_TASK_TYPE_TO_SELECTOR.put(RebalanceTask.RebalanceTaskType.PrimaryRebalance, PRIMARY_SELECTOR);
        MAP_TASK_TYPE_TO_SELECTOR.put(RebalanceTask.RebalanceTaskType.NormalRebalance, NORMAL_SELECTOR);
    }

    private static QuantifiableSelector<InstanceInfoImpl> getSelector(RebalanceTask.RebalanceTaskType taskType) {
        return MAP_TASK_TYPE_TO_SELECTOR.get(taskType);
    }

    public RebalanceSelector(Collection<InstanceInfoImpl> instanceInfoList) {
        this.instanceInfoList = new LinkedList<>(instanceInfoList);
        Collections.sort(this.instanceInfoList);
    }

    public ComparableRebalanceTask selectPrimaryRebalanceTask() throws NoNeedToRebalance {
        return selectRebalanceTaskBetweenInstances(instanceInfoList, RebalanceTask.RebalanceTaskType.PrimaryRebalance);
    }

    public ComparableRebalanceTask selectNormalRebalanceTask() throws NoNeedToRebalance {
        return selectRebalanceTaskBetweenInstances(instanceInfoList, RebalanceTask.RebalanceTaskType.NormalRebalance);
    }

    public ComparableRebalanceTask selectRebalanceTaskInsideInstance() throws NoNeedToRebalance {
        return selectRebalanceTaskInsideInstance(instanceInfoList);
    }

    private ComparableRebalanceTask selectRebalanceTaskBetweenInstances(Collection<InstanceInfoImpl> myInstanceInfoList,
            RebalanceTask.RebalanceTaskType taskType) throws NoNeedToRebalance {
        if (myInstanceInfoList.isEmpty()) {
            throw new NoNeedToRebalance();
        }

        Pair<Pair<InstanceInfoImpl, Double>, Pair<InstanceInfoImpl, Double>> minAndMax = getSelector(taskType)
                .selectTheMinAndMax(myInstanceInfoList, config.getPressureThreshold());
        logger.debug("min and max {}", minAndMax);
        if (minAndMax.getFirst() != null) {
            Pair<InstanceInfoImpl, Double> min = minAndMax.getFirst();
            InstanceInfoImpl instanceWithMinPressure = min.getFirst();

            LinkedList<InstanceInfoImpl> sourceList = new LinkedList<>();
            for (InstanceInfoImpl instance : myInstanceInfoList) {
                if (instance != instanceWithMinPressure) {
                    sourceList.addFirst(instance);
                }
            }
            List<InstanceInfo> destinations = new ArrayList<>();
            destinations.add(instanceWithMinPressure);
            for (InstanceInfoImpl source : sourceList) {
                try {
                    SimpleRebalanceTask rebalanceTask = (SimpleRebalanceTask) source
                            .selectARebalanceTask(destinations, taskType);
                    return new ComparableRebalanceTask(rebalanceTask.getMySourceSegmentUnit(),
                            rebalanceTask.getDestInstanceId(), config.getRebalanceTaskExpireTimeSeconds(),
                            min.getSecond(), taskType);
                } catch (NoSuitableTask e) {
                    logger.debug("there is no suitable {} task to {} from {}", taskType, instanceWithMinPressure,
                            source);
                }
            }
            logger.info("Oops there is no suitable {} task to the idle instance {}", taskType,
                    instanceWithMinPressure.getInstanceId());
            LinkedList<InstanceInfoImpl> infoList = new LinkedList<>(myInstanceInfoList);
            infoList.remove(instanceWithMinPressure);
            return selectRebalanceTaskBetweenInstances(infoList, taskType);
        }

        if (minAndMax.getSecond() != null) {
            Pair<InstanceInfoImpl, Double> max = minAndMax.getSecond();
            InstanceInfoImpl instanceWithMaxPressure = max.getFirst();
            logger.debug("got an overloaded instance, try to select a segment unit to remove {}, all instances {}",
                    instanceWithMaxPressure, myInstanceInfoList);
            try {
                List<InstanceInfo> destinationList = new ArrayList<>();
                for (InstanceInfoImpl instance : myInstanceInfoList) {
                    if (instance != instanceWithMaxPressure) {
                        destinationList.add(instance);
                    }
                }
                SimpleRebalanceTask rebalanceTask = (SimpleRebalanceTask) instanceWithMaxPressure
                        .selectARebalanceTask(destinationList, taskType);
                return new ComparableRebalanceTask(rebalanceTask.getMySourceSegmentUnit(),
                        rebalanceTask.getDestInstanceId(), config.getRebalanceTaskExpireTimeSeconds(), max.getSecond(),
                        taskType);
            } catch (NoSuitableTask e) {
                logger.info("Oops there is no suitable {} task from the overloaded instance {}", taskType,
                        instanceWithMaxPressure.getInstanceId());
                LinkedList<InstanceInfoImpl> infoList = new LinkedList<>(myInstanceInfoList);
                infoList.remove(instanceWithMaxPressure);
                return selectRebalanceTaskBetweenInstances(infoList, taskType);
            }
        }
        throw new NoNeedToRebalance();
    }

    private ComparableRebalanceTask selectRebalanceTaskInsideInstance(Collection<InstanceInfoImpl> myInstanceInfoList)
            throws NoNeedToRebalance {
        if (myInstanceInfoList.isEmpty()) {
            throw new NoNeedToRebalance();
        }

        TreeSet<ComparableRebalanceTask> tasks = new TreeSet<>();
        for (InstanceInfoImpl instanceInfo : myInstanceInfoList) {
            try {
                tasks.add(instanceInfo.selectAnInsideRebalanceTask());
            } catch (NoNeedToRebalance ignore) {
            }
        }
        if (tasks.isEmpty()) {
            throw new NoNeedToRebalance();
        } else {
            return tasks.first(); // TODO or last ?
        }
    }
}
