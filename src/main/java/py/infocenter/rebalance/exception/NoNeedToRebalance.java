package py.infocenter.rebalance.exception;

public class NoNeedToRebalance extends Exception {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public NoNeedToRebalance() {
        super();
    }

    public NoNeedToRebalance(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public NoNeedToRebalance(String message, Throwable cause) {
        super(message, cause);
    }

    public NoNeedToRebalance(String message) {
        super(message);
    }

    public NoNeedToRebalance(Throwable cause) {
        super(cause);
    }

}
