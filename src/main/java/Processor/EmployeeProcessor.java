package Processor;

import Metadata.CassandraMetadataReader;
import Util.FileSystemUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class EmployeeProcessor implements ApplicationProcessor{

    private final CassandraMetadataReader cassandraMetadataReader = new CassandraMetadataReader();
    private Dataset<Row> employeeTable;

    @Override
    public void startProcessor() throws Exception{
        getTables();

        Dataset<Row> managerDS = employeeTable.filter(employeeTable.col("title").isin("Manager", "Supervisor", "Owner"));
        Dataset<Row> workerDS = employeeTable.filter(employeeTable.col("title").equalTo("Worker"));

        FileSystemUtil.writeFileOutputLocation(managerDS, "ManagerList");
        FileSystemUtil.writeFileOutputLocation(workerDS, "WorkerList");

    }

    @Override
    public void getTables() {
        employeeTable = cassandraMetadataReader.getEmployees();
    }
}
