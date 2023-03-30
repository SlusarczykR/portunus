package org.slusarczykr.portunus.cache.exception;

public class InvalidPortunusStateException extends IllegalStateException {

    public InvalidPortunusStateException(String message) {
        super(message);
    }

    public InvalidPortunusStateException(String message, Throwable cause) {
        super(message, cause);
    }
}
