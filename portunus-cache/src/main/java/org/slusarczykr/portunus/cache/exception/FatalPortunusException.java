package org.slusarczykr.portunus.cache.exception;

public class FatalPortunusException extends RuntimeException {

    public FatalPortunusException(String message) {
        super(message);
    }

    public FatalPortunusException(String message, Throwable cause) {
        super(message, cause);
    }
}
