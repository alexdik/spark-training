package spark.workshop.task1;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import spark.workshop.util.SparkHelper;

import static org.apache.spark.sql.functions.*;

public class CostReport {
    private SparkHelper spark = new SparkHelper();
    private Dataset<Row> src = spark.getSession().read().json("data/ad-response.json");

    public void showSource() {
        src.show(false);
        src.printSchema();
    }

    /*
        TODO: This report should provide total sum paid for ads delivered to 3 advertisers (ids: 8, 9, 10).

        Your output dataset should look as following:
            +------------+----+
            |advertiserId|sum |
            +------------+----+
            |9           |1273|
            |10          |702 |
            |8           |419 |
            +------------+----+

        Hints:
            • filter dataset to get only valid advertisers: 8, 9, 19
            • group dataset by advertiserId column
            • calculate total sum for all auctions
            • update resulting column names as needed

    */
    public Dataset<Row> build() {
        return src;
    }
}
