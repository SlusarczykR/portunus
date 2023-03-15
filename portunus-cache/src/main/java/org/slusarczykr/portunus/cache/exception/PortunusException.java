package org.slusarczykr.portunus.cache.exception;

public class PortunusException extends Exception {

    public PortunusException(String message) {
        super(message);
    }

    public PortunusException(String message, Throwable cause) {
        super(message, cause);
    }
}
