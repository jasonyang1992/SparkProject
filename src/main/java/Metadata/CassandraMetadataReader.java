package Metadata;

import Util.AppConstants;
import Util.PropertyUtil;
import Util.SparkUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class CassandraMetadataReader implements MetadataReader{


    @Override
    public Dataset<Row> getEmployees() {
        Dataset<Row> employeeTable = getCassandraTable(PropertyUtil.getProperty("Cassandra.Table.Employees"));
        employeeTable = employeeTable.filter(employeeTable.col("startdate").leq(AppConstants.TODAY_DATE).and(employeeTable.col("enddate").geq(AppConstants.TODAY_DATE)));
        return employeeTable;
    }

    @Override
    public Dataset<Row> getJobs() {
        return getCassandraTable(PropertyUtil.getProperty("Cassandra.Table.Jobs"));
    }

    @Override
    public Dataset<Row> getHourlyRate() {
        return getCassandraTable(PropertyUtil.getProperty("Cassandra.Table.HourlyRate"));
    }

    private Dataset<Row> getCassandraTable(String tableName){

        Dataset<Row> cassandraTableDS = SparkUtil.getSparkSession().read().format("org.apache.spark.sql.cassandra")
                .option("keyspace", PropertyUtil.getProperty("Cassandra.Keyspace"))
                .option("table", tableName)
                .load();

        cassandraTableDS.createOrReplaceTempView(tableName);

        return cassandraTableDS;
    }
}
