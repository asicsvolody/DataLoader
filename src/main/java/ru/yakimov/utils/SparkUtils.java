/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 *
 * Класс статических методов Spark
 */

package ru.yakimov.utils;


import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import ru.yakimov.BootProcessMain;
import ru.yakimov.LogDB.Log;
import ru.yakimov.config.JobConfiguration;
import java.util.*;

public class SparkUtils {

    /**
     * Метод получения отформатированных под Hive types String представления полей
     * @param dir
     * @param jobConfig
     * @return
     * @throws Exception
     */
    public static List<String> getFormattingColsFromDir(String dir, JobConfiguration jobConfig) throws Exception {

        SparkSession spark = BootProcessMain.CONTEXT.getBean(SparkSession.class);

        Log.write(jobConfig, "Spark read schema from dir "+ dir);
        StructType schema = spark
                .read()
                .parquet(dir + BootProcessMain.SEPARATOR + "*.parquet").schema();

        Log.write(jobConfig, schema.toString());

        List<String> colsList = LoaderUtils.getFormattingCols(schema);

        Log.write(jobConfig, "Checking portions fields");

        LoaderUtils.checkPartitions(colsList, jobConfig, dir);

        return colsList;
    }
}
