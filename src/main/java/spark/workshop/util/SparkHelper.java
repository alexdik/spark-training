package spark.workshop.util;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class SparkHelper {
    private final SparkSession session;

    public SparkHelper() {
        SparkConf conf = new SparkConf()
                .setMaster("local[4]")
                .setAppName("spark-workshop");
        this.session = SparkSession.builder().config(conf).getOrCreate();
    }

    public SparkSession getSession() {
        return session;
    }
}
