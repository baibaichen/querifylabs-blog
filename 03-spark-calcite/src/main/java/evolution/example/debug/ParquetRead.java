package evolution.example.debug;

import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.execution.datasources.parquet.VectorizedParquetRecordReader;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;
import java.util.List;

@Slf4j
public class ParquetRead {


    @AllArgsConstructor
    @Getter
    static class TestData {
        List<String> cols;
        String parquetFile;

        static TestData spdb() {
            return new TestData(ImmutableList.of(
                    "316", "374", "63", "242", "243", "244", "245", "246", "247", "248", "249",
                    "250", "251", "252", "253", "254", "255", "256", "257", "258", "259",
                    "260", "261", "262", "263", "264", "265", "266", "267", "268", "269",
                    "270", "271", "272", "273"),
                    "/Users/chang.chen/test/data/1024_1024.snappy.parquet");
        }

        static TestData spark3_build() {
            return new TestData(ImmutableList.of("36", "1"),
                    "/Users/chang.chen/test/data/tnt.snappy.parquet");
        }
    }

    public static void main(String[] args) throws IOException {
        log.info("hello");
        SQLConf sqlConf = new SQLConf();
        for (int i=0; i< 1; i++){
            //read(TestData.spdb(), sqlConf.offHeapColumnVectorEnabled(), sqlConf.parquetVectorizedReaderBatchSize());
            read(TestData.spark3_build(), sqlConf.offHeapColumnVectorEnabled(), sqlConf.parquetVectorizedReaderBatchSize());
        }
    }

    private static
    void read(TestData data,
              boolean enableOffHeapColumnVector,
              int vectorizedReaderBatchSize) throws IOException {
        long result;
        try (VectorizedParquetRecordReader reader =
                     new VectorizedParquetRecordReader(enableOffHeapColumnVector, vectorizedReaderBatchSize)) {
            reader.initialize(data.parquetFile, data.cols);
            ColumnarBatch batch = reader.resultBatch();
            ColumnVector col = batch.column(0);
            result = 0;
            while (reader.nextBatch()) {
                int numRows = batch.numRows();
                int i = 0;
                while (i < numRows) {
                    result += col.getLong(i);
                    i += 1;
                }
            }
        }
        log.info(String.valueOf(result));
    }


}
