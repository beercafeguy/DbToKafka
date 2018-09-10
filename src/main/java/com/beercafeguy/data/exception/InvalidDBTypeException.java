package com.beercafeguy.data.exception;

public class InvalidDBTypeException extends RuntimeException{

    public InvalidDBTypeException(String message) {
        super(message);
    }
}
