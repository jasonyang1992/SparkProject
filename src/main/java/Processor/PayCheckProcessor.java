package Processor;

import Metadata.CassandraMetadataReader;
import Metadata.MetadataReader;
import Util.FileSystemUtil;
import Util.PropertyUtil;

import Util.SparkUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;


public class PayCheckProcessor implements ApplicationProcessor{

    private final MetadataReader cassandraMetadataReader = new CassandraMetadataReader();
    private Dataset<Row> employees;
    private Dataset<Row> hourlyRates;

    @Override
    public void startProcessor() throws Exception{
        getTables();

        //Join dataset to match employee occupation to pay
        Dataset<Row> joinSource = employees.join(hourlyRates, employees.col("title").equalTo(hourlyRates.col("title")));
        joinSource = joinSource.select(employees.col("*"), hourlyRates.col("pay"));

        //Create a new update structure dataset
        StructType structType = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("Name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("Title", DataTypes.StringType, false, Metadata.empty()),
                new StructField("Pay", DataTypes.DoubleType, false, Metadata.empty())
        });
        List<Row> updatedTimesheet = new ArrayList<>();

        //Join timesheet data
        Dataset<Row> timesheet = FileSystemUtil.readFileInput("timesheet");
        timesheet = timesheet.join(joinSource, timesheet.col("workerid").equalTo(joinSource.col("workerid")));
        timesheet = timesheet.select(joinSource.col("*"), timesheet.col("hourswork"));

        List<Row> timesheetList = timesheet.collectAsList();

        // Loop the timesheet to generate new data set
        // Multiple pay by hours worked
        for(Row row : timesheetList){
            updatedTimesheet.add(RowFactory.create(row.getInt(row.fieldIndex("workerid")),
                                row.getString(row.fieldIndex("name")),
                                row.getString(row.fieldIndex("title")),
                                row.getDouble(row.fieldIndex("pay"))*8));
        }
        //Create thew new Dataset
        Dataset<Row> timesheetDS = SparkUtil.getSparkSession().createDataFrame(updatedTimesheet, structType);

        //Create the file
        FileSystemUtil.writeToFile(timesheetDS, PropertyUtil.getProperty("File.Output.Location"), PropertyUtil.getProperty("File.Employee.Payroll"));

    }

    @Override
    public void getTables() {
        //Load Database
        employees = cassandraMetadataReader.getEmployees();
        hourlyRates = cassandraMetadataReader.getHourlyRate();
    }
}
