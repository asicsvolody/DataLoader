/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 */

package ru.yakimov.Jobs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import ru.yakimov.Assets;
import ru.yakimov.MySqlDB.Log;
import ru.yakimov.utils.HdfsUtils;
import ru.yakimov.utils.HiveUtils;
import ru.yakimov.utils.SparkUtils;

import java.util.*;

public class JoinAnyBaseInOne extends Job {
    @Override
    public Integer call() throws Exception {

        SparkSession spark = Assets.getInstance().getSpark();

        Log.write(jobConfig, "Checking directory to");

        HdfsUtils.deleteDirWithLog(jobConfig, jobConfig.getDirTo());

        Log.write(jobConfig, "Read columns from data");

        Set<String> fullColsSet = new HashSet<>();

        for (String dir : jobConfig.getDirFrom()) {
            Log.write(jobConfig, "Spark read schema from dir "+ dir);
            StructType schema = spark
                    .read()
                    .parquet(dir + Assets.SEPARATOR + "*.parquet").schema();
            Log.write(jobConfig, "Add columns to  colsSet");

            fullColsSet.addAll(HiveUtils.getFormattingCols(schema));
        }

        Log.write(jobConfig, "Creating hive table");

        HiveUtils.createHiveTable(jobConfig, fullColsSet.toArray(new String[0]));

        Log.write(jobConfig, "Dynamic insert into hiveTable");

        spark.sql(String.format("describe %s.%s",jobConfig.getDbConfiguration().getSchema(), jobConfig.getDbConfiguration().getTable())).show();



        for (String dir : jobConfig.getDirFrom()) {
            Log.write(jobConfig, "Spark read data from dir "+ dir);
            Dataset<Row> data = spark.read()
                    .parquet(dir + Assets.SEPARATOR + "*.parquet");

            data.createOrReplaceTempView("tmp_table");

            data.show();


            String cols = String.join(","
                    , SparkUtils
                            .getArrayWithColsAndNull(
                                    fullColsSet.toArray(new String[0])
                                    , HiveUtils.getFormattingCols(data.schema())
                            )
            );



            Log.write(jobConfig, "Write data to Hive Table");

            System.out.println(String.format(
                    "INSERT INTO %s.%s SELECT %s FROM tmp_table"
                    , jobConfig.getDbConfiguration().getSchema()
                    ,jobConfig.getDbConfiguration().getTable()
                    ,cols
            ));

            spark.sql(String.format(
                    "INSERT INTO %s.%s SELECT %s FROM tmp_table"
                    , jobConfig.getDbConfiguration().getSchema()
                    ,jobConfig.getDbConfiguration().getTable()
                    ,cols
            ));

            spark.sql(String.format("SELECT * FROM %s.%s",jobConfig.getDbConfiguration().getSchema(),jobConfig.getDbConfiguration().getTable()))
                    .sort("user_id")
                    .show();

        }

        spark.sql(String.format("SELECT * FROM %s.%s",jobConfig.getDbConfiguration().getSchema(),jobConfig.getDbConfiguration().getTable()))
                .sort("user_id")
                .show();
        return 0;
    }
}
