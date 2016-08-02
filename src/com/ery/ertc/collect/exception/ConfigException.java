package com.ery.ertc.collect.exception;

public class ConfigException extends RuntimeException{

    public ConfigException() {
        super();
    }

    public ConfigException(final String message) {
        super(message);
    }

    public ConfigException(final String message, final Throwable t) {
        super(message, t);
    }

    public ConfigException(final Throwable t) {
        super(t);
    }

}
