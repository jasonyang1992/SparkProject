package Metadata;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface MetadataReader {

    Dataset<Row> getEmployees();
    Dataset<Row> getJobs();
    Dataset<Row> getHourlyRate();
}
