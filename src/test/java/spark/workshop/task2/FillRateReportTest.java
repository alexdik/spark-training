package spark.workshop.task2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.assertj.core.data.Offset;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FillRateReportTest {
    private static Dataset<Row> ds;

    @BeforeClass
    public static void setUp() {
        FillRateReport fillRateReport = new FillRateReport();
        fillRateReport.showSource();
        ds = fillRateReport.build();
        ds.cache();
    }

    @Test
    public void bestFillRatePublisher() {
        Row firstRow = ds.collectAsList().get(0);
        assertThat(firstRow.<Long>getAs("publisherId")).isEqualTo(7);
        assertThat(firstRow.<Double>getAs("fillRate")).isCloseTo(1.0, Offset.offset(0.1));
    }

    @Test
    public void worstFillRatePublisher() {
        Row firstRow = ds.collectAsList().get((int) Math.max(ds.count() - 1, 0));
        assertThat(firstRow.<Long>getAs("publisherId")).isEqualTo(5);
        assertThat(firstRow.<Double>getAs("fillRate")).isCloseTo(0.4, Offset.offset(0.1));
    }

    @Test
    public void has2Columns() {
        assertThat(ds.columns()).containsOnly("publisherId", "fillRate");
    }
}
