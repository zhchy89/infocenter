package py.infocenter.rebalance.exception;

public class NoSegmentUnitCanBeRemoved extends Exception {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public NoSegmentUnitCanBeRemoved() {
        super();
    }

    public NoSegmentUnitCanBeRemoved(String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public NoSegmentUnitCanBeRemoved(String message, Throwable cause) {
        super(message, cause);
    }

    public NoSegmentUnitCanBeRemoved(String message) {
        super(message);
    }

    public NoSegmentUnitCanBeRemoved(Throwable cause) {
        super(cause);
    }

}
