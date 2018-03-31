package spark.workshop.util;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkHelper {
    public final SparkSession session;
    public final SparkContext context;

    public SparkHelper() {
        SparkConf conf = new SparkConf()
            .setMaster("local[4]")
            .setAppName("spark-workshop");
        this.session = SparkSession.builder().config(conf).getOrCreate();
        this.context = this.session.sparkContext();
    }

}
