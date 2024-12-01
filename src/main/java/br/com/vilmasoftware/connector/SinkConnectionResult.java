package br.com.vilmasoftware.connector;

import lombok.ToString;

@ToString
public class SinkConnectionResult {
    private long rowsCopied;
    private Exception exception;

    public SinkConnectionResult(long rowsCopied) {
        this.rowsCopied = rowsCopied;
    }

    public SinkConnectionResult(Exception exception) {
        this.exception = exception;
    }
}
