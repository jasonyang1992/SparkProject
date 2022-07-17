package Processor;

import Metadata.CassandraMetadataReader;
import Metadata.MetadataReader;
import Util.FileSystemUtil;
import Util.PropertyUtil;
import Util.SparkUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.List;

public class PayCheckProcessor implements ApplicationProcessor{

    private final MetadataReader cassandraMetadataReader = new CassandraMetadataReader();
    private Dataset<Row> employees;
    private Dataset<Row> hourlyRates;

    @Override
    public void startProcessor() {
        getTables();

        Dataset<Row> joinSource = employees.join(hourlyRates, employees.col("title").equalTo(hourlyRates.col("title")));
        joinSource = joinSource.select(employees.col("*"), hourlyRates.col("pay"));
        try {
            Dataset<Row> timesheet = FileSystemUtil.readFileInput("timesheet");
            timesheet = timesheet.join(joinSource, timesheet.col("workerid").equalTo(joinSource.col("workerid")));
            timesheet = timesheet.select(joinSource.col("*"), timesheet.col("hourswork"));
            FileSystemUtil.writeToFile(timesheet, PropertyUtil.getProperty("File.Output.Location"), PropertyUtil.getProperty("File.Employee.Payroll"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void getTables() {
        employees = cassandraMetadataReader.getEmployees();
        hourlyRates = cassandraMetadataReader.getHourlyRate();
    }
}
