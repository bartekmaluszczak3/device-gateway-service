package com.device.service.exeption;

import org.springframework.http.HttpStatus;

public class WellKnownException extends RuntimeException {

    private final HttpStatus httpStatus;

    public WellKnownException(String message, HttpStatus httpStatus) {
        super(message);
        this.httpStatus = httpStatus;
    }

    public WellKnownException(String message, HttpStatus httpStatus, Throwable cause) {
        super(message, cause);
        this.httpStatus = httpStatus;
    }

    public HttpStatus getHttpStatus() {
        return httpStatus;
    }
}