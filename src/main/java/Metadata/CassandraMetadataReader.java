package Metadata;

import Util.PropertyUtil;
import Util.SparkUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class CassandraMetadataReader implements MetadataReader{


    @Override
    public Dataset<Row> getEmployees() {
        return getCassandraTable(PropertyUtil.getProperty("Cassandra.Table.Employees"));
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
