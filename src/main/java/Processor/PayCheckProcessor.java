package Processor;

import Metadata.CassandraMetadataReader;
import Metadata.MetadataReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class PayCheckProcessor implements ApplicationProcessor{

    private MetadataReader cassandraMetadataReader = new CassandraMetadataReader();
    private Dataset<Row> employees;

    @Override
    public void startProcessor() {
        getTables();

    }

    @Override
    public void getTables() {
        employees = cassandraMetadataReader.getEmployees();
    }
}
