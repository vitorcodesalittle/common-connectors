package br.com.vilmasoftware.connector;
import lombok.*;

import java.io.File;
import java.nio.file.Path;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class SourceConnectionResult {
    private long rowCount;
    private File csvTableContent;
    private File csvTableDataTypeDictionary;
}
