package com.ery.ertc.collect.exception;

public class CollectException extends RuntimeException{

    public CollectException() {
        super();
    }

    public CollectException(final String message) {
        super(message);
    }

    public CollectException(final String message, final Throwable t) {
        super(message, t);
    }

    public CollectException(final Throwable t) {
        super(t);
    }

}
