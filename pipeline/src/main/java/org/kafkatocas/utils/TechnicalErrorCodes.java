package org.json2kafka.utils;

public enum TechnicalErrorCodes {

    FAILED_TO_FIND_LOCATION_IN_REFERENCE(10001),
    FAILED_TO_FIND_LOCATION_INFORMATION_IN_INPUT(10002),
    INPUT_RECORD_IS_NULL(10003),
    FAILED_TO_LOAD_REFERENCE_FILE(10004);

    private final int errorCode;

    TechnicalErrorCodes(int errorCode) {
        this.errorCode = errorCode;
    }

    public int getErrorCode() {
        return errorCode;
    }
}
