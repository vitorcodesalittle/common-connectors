package br.com.vilmasoftware.connector.impl;

import br.com.vilmasoftware.connector.SinkConnector;
import br.com.vilmasoftware.connector.SourceConnectionResult;
import br.com.vilmasoftware.writers.CSVLineWriter;
import lombok.AllArgsConstructor;
import lombok.Getter;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

public abstract class BaseSinkConnector implements SinkConnector {


}
