package org.json2kafka.utils;

public class TechnicalException extends RuntimeException {

    public TechnicalException(String errorCode, Throwable e) {
        super(errorCode, e);
    }
}
