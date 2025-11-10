package io.aiven.inkless.cache;

/* When this Exception is raised by a LogFragment, it means that the data contained in the LogFragment
   is stale and it needs to be invalidated. */
public class StaleLogFragmentException extends Exception {
    public StaleLogFragmentException(String message) {
        super(message);
    }

    /* avoid the expensive and useless stack trace */
    @Override
    public Throwable fillInStackTrace() {
        return this;
    }
}
