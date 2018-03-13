package spark.workshop.task1;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.collection.mutable.WrappedArray;
import java.util.Collection;
import static org.apache.spark.sql.functions.*;
import static org.assertj.core.api.Assertions.*;
import scala.collection.JavaConverters;

public class CostReportTest {
    private static Dataset<Row> ds;

    @BeforeClass
    public static void setUp() {
        CostReport costReport = new CostReport();
        costReport.showSource();
        ds = costReport.build();
    }

    @Test
    public void totalSum() {
        assertThat(
            ds.groupBy().sum("sum").collectAsList().get(0).get(0)
        ).isEqualTo(5725L);
    }

    @Test
    public void contains3Advertisers() {
        WrappedArray<Long> scalaArray = ds.groupBy().agg(collect_list("advertiserId")).collectAsList().get(0).getAs(0);
        Collection<Long> advertisers = JavaConverters.asJavaCollectionConverter(scalaArray).asJavaCollection();
        assertThat(advertisers).containsOnly(8L, 9L, 10L);
    }

    @Test
    public void has2Columns() {
        assertThat(ds.columns()).containsOnly("sum", "advertiserId");
    }

}
