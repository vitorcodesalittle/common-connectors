package br.com.vilmasoftware.connector;
import lombok.*;

@Getter
@Setter
@ToString
public class SourceConnectionResult {
    private long rowCount;
    private Exception exception;

    public SourceConnectionResult() {}

    public SourceConnectionResult(long rowCount) {
        this.rowCount = rowCount;
    }


    public SourceConnectionResult(Exception exception) {
        this.exception = exception;
    }
}
