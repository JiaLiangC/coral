package com.linkedin.coral.coralservice.apps.err;

public interface ErrorHandler {
    void handleError(Exception e);
    void logWarning(String message);
}