package com.ery.ertc.collect.exception;

public class ClusterException extends RuntimeException{

    public ClusterException() {
        super();
    }

    public ClusterException(final String message) {
        super(message);
    }

    public ClusterException(final String message, final Throwable t) {
        super(message, t);
    }

    public ClusterException(final Throwable t) {
        super(t);
    }
    
}
