/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 */

package ru.yakimov.Jobs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Component;
import ru.yakimov.Assets;
import ru.yakimov.MySqlDB.Log;
import ru.yakimov.utils.HdfsUtils;
import ru.yakimov.utils.HiveTable;
import ru.yakimov.utils.LoaderUtils;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Component
public class PartitionSparkDataJob extends Job {

    @Override
    public Integer call() throws Exception {
        SparkSession spark = Assets.getInstance().getSpark();

        if(jobConfig.getDirFrom().size()!= 1){
            Log.write(jobConfig, "Wrong dir from array size", Log.Level.ERROR);
            return 1;
        }

        Log.write(jobConfig, "Checking for a directory ");

        HdfsUtils.deleteDirWithLog(jobConfig, jobConfig.getDirTo());

        Log.write(jobConfig, "Spark read data from dir "+ jobConfig.getDirFrom().get(0));


//        spark.sql("CREATE DATABASE IF NOT EXISTS hiveData LOCATION '/myHive'");
//
//        spark.sql("CREATE EXTERNAL TABLE IF NOT EXISTS hiveData.usersDB_users \n" +
//                " (user_id string, user_name string, user_phone string, user_marital_status string) \n" +
//                " PARTITIONED BY (user_age string)\n" +
//                " STORED AS PARQUET \n" +
//                " LOCATION '/myHive/usersData' ");
//
//        spark.sql("INSERT INTO hiveData.usersDB_users PARTITION(user_age=31) VALUES ('1','Irina', '124421232', 'true')");
//        spark.sql("SELECT * FROM hiveData.usersDB_users").show();





        Dataset<Row> data= Assets.getInstance().getSpark()
                .read()
                .parquet(jobConfig.getDirFrom().get(0) + Assets.SEPARATOR + "*.parquet");

        Log.write(jobConfig, "Creating hive table");

        HiveTable.createHiveTable(jobConfig, data.schema());

        data.createOrReplaceTempView("tmp_table");

        data.show();

        String partitions = String.join(",",jobConfig.getPartitions());
        String usualCols = String.join(",", LoaderUtils.getUsualCols(data.schema(), jobConfig.getPartitions()));

        System.out.println(String.format(
                "INSERT INTO %s.%s PARTITION(%s) SELECT %s,%s FROM tmp_table"
                , jobConfig.getDbConfiguration().getSchema()
                ,jobConfig.getDbConfiguration().getTable()
                ,partitions
                ,usualCols
                ,partitions
        ));

        spark.sql(String.format(
                "INSERT INTO %s.%s PARTITION(%s) SELECT %s,%s FROM tmp_table"
                , jobConfig.getDbConfiguration().getSchema()
                ,jobConfig.getDbConfiguration().getTable()
                ,partitions
                ,usualCols
                ,partitions
        ));

        spark.sql(String.format("SELECT * FROM %s.%s"
                , jobConfig.getDbConfiguration().getSchema()
                ,jobConfig.getDbConfiguration().getTable() )).show();


//        String jsonType = data.schema().json();
//
//        Log.write(jobConfig, "Have created schema json: " + jsonType);
//
//        Log.write(jobConfig, "Spark write partition data to dir " + jobConfig.getDirTo());
//        data.write()
//            .partitionBy(jobConfig.getPartitions())
//            .parquet(jobConfig.getDirTo());
//
//        Log.write(jobConfig, "Writing struct.json");
//
//        if(!HdfsUtils.writeToHdfs(jobConfig.getDirTo()+Assets.SEPARATOR+Assets.STRUCT_JSON_FILE, jsonType)){
//            Log.write(jobConfig, "Struct file dont.write ", Log.Level.ERROR);
//            return 1;
//        }
//
//        Log.write(jobConfig, "Delete tmp directory");
//
//        HdfsUtils.deleteDirWithLog(jobConfig, jobConfig.getDirFrom().get(0));

        return 0;
    }
}
