package Util;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class SparkUtil {

    private static SparkConf sparkConf;
    private static SparkSession sparkSession;

    private static void sparkConf(){
        sparkConf = new SparkConf();
        sparkConf.setAppName("SparkBatchProject")
                .set("spark.cassandra.connection.host", "127.0.0.1")
                .set("spark.cassandra.connection.port", "9042")
                .set("spark.cassandra.auth.username", "cassandra")
                .set("spark.cassandra.auth.password", "cassandra")
                .setMaster("local[*]");
    }

    public static SparkSession getSparkSession(){
        if(sparkSession == null){
            if(sparkConf == null){
                sparkConf();
            }
            sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        }
        return sparkSession;
    }
}
