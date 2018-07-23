package py.infocenter.service;

import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import py.infocenter.common.InfoCenterConstants;

@Deprecated
public class DynamicConfigWrapper {
    private static final Logger logger = Logger.getLogger(DynamicConfigWrapper.class);
    private Map<String, String> dynamicParameters;

    public Map<String, String> getDynamicParameters() {
        return dynamicParameters;
    }

    private DynamicConfigWrapper() {
        dynamicParameters = new ConcurrentHashMap<String, String>();
        // initialize this class by spring configuration files
        dynamicParameters.put("log.level", "DEBUG");
        dynamicParameters.put("volumeToBeCreatedTimeout",
                String.valueOf(InfoCenterConstants.getVolumeToBeCreatedTimeout()));
        dynamicParameters.put("segmentUnitReportTimeout",
                String.valueOf(InfoCenterConstants.getSegmentUnitReportTimeout()));
        dynamicParameters.put("timeOfdeadVolumeToRemove",
                String.valueOf(InfoCenterConstants.getTimeOfdeadVolumeToRemove()));

    }

    private static class LazyHolder {
        private static final DynamicConfigWrapper singletonInstance = new DynamicConfigWrapper();
    }

    public static DynamicConfigWrapper getInstance() {
        return LazyHolder.singletonInstance;
    }

    public void setParammeter(String name, String value) {
        boolean matchKey = false;

        // apply the change
        if (name.equalsIgnoreCase("log.level")) {
            logger.debug("leve change to " + value);
            LogManager.getRootLogger().setLevel(Level.toLevel(value));
            matchKey = true;
        } else if (name.equalsIgnoreCase("volumeToBeCreatedTimeout")) {
            int volumeToBeCreatedTimeout = Integer.parseInt(value);
            InfoCenterConstants.setVolumeToBeCreatedTimeout(volumeToBeCreatedTimeout);
            matchKey = true;
        } else if (name.equalsIgnoreCase("segmentUnitReportTimeout")) {
            int segmentUnitReportTimeout = Integer.parseInt(value);
            InfoCenterConstants.setSegmentUnitReportTimeout(segmentUnitReportTimeout);
            matchKey = true;
        } else if (name.equalsIgnoreCase("timeOfdeadVolumeToRemove")) {
            int timeOfdeadVolumeToRemove = Integer.parseInt(value);
            InfoCenterConstants.setTimeOfdeadVolumeToRemove(timeOfdeadVolumeToRemove);
            matchKey = true;
        } else {
            logger.debug("can't set this parameter " + name);
        }

        if (matchKey) {
            // store the change of this parameter.
            dynamicParameters.put(name, value);
        }
    }
}
