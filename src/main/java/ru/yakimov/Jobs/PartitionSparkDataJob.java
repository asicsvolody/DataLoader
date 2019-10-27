/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 *
 * Задание сгрузки данных из директории hdfs в таблицу hive c партиционированием по полям
 */

package ru.yakimov.Jobs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Component;
import ru.yakimov.Assets;
import ru.yakimov.MySqlDB.Log;
import ru.yakimov.utils.HdfsUtils;
import ru.yakimov.utils.HiveUtils;
import ru.yakimov.utils.LoaderUtils;

@Component
public class PartitionSparkDataJob extends Job {

    @Override
    public Integer call() throws Exception {
        SparkSession spark = Assets.getInstance().getSpark();

        if(jobConfig.getDirFrom().length!= 1){
            Log.write(jobConfig, "Wrong dir from array size", Log.Level.ERROR);
            return 1;
        }

        Log.write(jobConfig, "Checking directory to");

        HdfsUtils.deleteDirWithLog(jobConfig, jobConfig.getDirTo());

        Log.write(jobConfig, "Spark read data from dir "+ jobConfig.getDirFrom()[0]);

        Dataset<Row> data= spark
                .read()
                .parquet(jobConfig.getDirFrom()[0] + Assets.SEPARATOR + "*.parquet");

        Log.write(jobConfig, "Creating hive table");

        HiveUtils.createHiveTable(jobConfig, data.schema());

        data.createOrReplaceTempView("tmp_table");

        data.show();

        String partitions = String.join(",", LoaderUtils.getColumnNameOnly(jobConfig.getPartitions()));

        String usualCols = String.join(",", LoaderUtils.getUsualColumns(data.schema(), jobConfig.getPartitions()));

        Log.write(jobConfig, "Write data to Hive Table");

        spark.sql(String.format(
                "INSERT INTO %s.%s PARTITION(%s) SELECT %s,%s FROM tmp_table"
                , jobConfig.getDbConfiguration().getSchema()
                ,jobConfig.getDbConfiguration().getTable()
                ,partitions
                ,usualCols
                ,partitions
        ));

        return 0;
    }
}
