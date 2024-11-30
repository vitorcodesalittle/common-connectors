package br.com.vilmasoftware.connector;
import lombok.*;

import java.io.File;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class SourceConnectionResult {
    private long rowCount;
    private File csvTableContent;
    private File csvTableDataTypeDictionary;
}
