package py.infocenter.rebalance;

public class RebalanceConfiguration {

    private int pressureAddend = 0;

    private double pressureThreshold = 0.2;

    private int rebalanceTaskExpireTimeSeconds = 600;

    private int segmentWrapSize = 10;

    private static RebalanceConfiguration instance = new RebalanceConfiguration();

    public static RebalanceConfiguration getInstance() {
        return instance;
    }

    public int getPressureAddend() {
        return pressureAddend;
    }

    public void setPressureAddend(int pressureAddend) {
        this.pressureAddend = pressureAddend;
    }

    public double getPressureThreshold() {
        return pressureThreshold;
    }

    public void setPressureThreshold(double pressureThreshold) {
        this.pressureThreshold = pressureThreshold;
    }

    public int getRebalanceTaskExpireTimeSeconds() {
        return rebalanceTaskExpireTimeSeconds;
    }

    public void setRebalanceTaskExpireTimeSeconds(int rebalanceTaskExpireTimeSeconds) {
        this.rebalanceTaskExpireTimeSeconds = rebalanceTaskExpireTimeSeconds;
    }

    public int getSegmentWrapSize() {
        return segmentWrapSize;
    }

    public void setSegmentWrapSize(int segmentWrapSize) {
        this.segmentWrapSize = segmentWrapSize;
    }
}
