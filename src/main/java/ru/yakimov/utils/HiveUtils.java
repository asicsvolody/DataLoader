/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 *
 * Класс статическич Hive методов
 */

package ru.yakimov.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.types.StructType;
import ru.yakimov.Assets;
import ru.yakimov.MySqlDB.Log;
import ru.yakimov.config.JobConfiguration;

import java.util.List;
import java.util.Set;


public class HiveUtils {

    /**
     * Динамический insert в таблицу hive c автоматическим заполнением недостающий полей значениями null
     *
     * @param jobConf
     * @param dirFrom
     * @throws Exception
     */
    public static void insetToHiveTable(JobConfiguration jobConf, String dirFrom ) throws Exception {
        SparkSession spark = Assets.getInstance().getSpark();
        String databaseTo = jobConf.getDbConfiguration().getSchema();
        String tableTo = jobConf.getDbConfiguration().getTable();

        Log.write(jobConf, "Read table columns");

        List<String> tableCols = spark
                .sql(String.format("DESC %s.%s",databaseTo,tableTo))
                .toJavaRDD()
                .map(row -> row.getString(0).trim())
                .filter(v -> !v.startsWith("#"))
                .collect();

        Log.write(jobConf, tableCols.toString());

        Log.write(jobConf, "Spark read data from dir "+ dirFrom);

        Dataset<Row> data = spark.read()
                .parquet(dirFrom + Assets.SEPARATOR + "*.parquet");

        data.createOrReplaceTempView("tmp_table");

        data.show();

        String usualCols = String.join(","
                , LoaderUtils.getColsAndNullNoPartitions(
                        tableCols,
                        data.schema().fieldNames(),
                        jobConf.getPartitions()
                )

        );

        System.out.println(usualCols);

        String partitionsCols= String.join(", ",LoaderUtils.getColumnNameOnly(jobConf.getPartitions()));

        Log.write(jobConf, "Write data to Hive Table");

        String hiveScript = String.format(
                "INSERT INTO %s.%s PARTITION(%s) SELECT %s,%s FROM tmp_table"
                , databaseTo
                ,tableTo
                ,partitionsCols
                ,usualCols
                ,partitionsCols
        );

        Log.write(jobConf, hiveScript);

        spark.sql(hiveScript);

    }


    /**
     * Создание Hive таблицы из массива полей
     *
     * @param jConfig
     * @param columnsArr
     * @throws Exception
     */
    public static synchronized void createHiveTable(JobConfiguration jConfig, String[] columnsArr) throws Exception {
        SparkSession spark = Assets.getInstance().getSpark();

        String schema = jConfig.getDbConfiguration().getSchema();

        if(schema == null){
            throw new Exception("There is not schema name");
        }

        String table = jConfig.getDbConfiguration().getTable();

        if(table == null){
            throw new Exception("There is not table name");
        }

        Log.write(jConfig, "Creating database: "+ schema);

        spark.sql(String.format("CREATE DATABASE IF NOT EXISTS %s LOCATION '/%s'",schema, schema));

        Log.write(jConfig, "Creating table: " +table);

        Log.write(jConfig, "Delete old table if exist");

        spark.sql(String.format("DROP TABLE IF EXISTS %s.%s", schema, jConfig.getDbConfiguration().getTable()));

        HdfsUtils.deleteDirWithLog(jConfig, Assets.SEPARATOR +schema + Assets.SEPARATOR+ table);


        String cols = String.join(", ", LoaderUtils.getUsualColumns(columnsArr, jConfig.getPartitions()));

        String partitions = String.join(",", jConfig.getPartitions());

        System.out.println(cols);

        String hiveScript = String.format("CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s \n" +
                " (%s) \n" +
                " PARTITIONED BY (%s)\n" +
                " STORED AS PARQUET \n" +
                " LOCATION '/%s/%s' ",schema, table, cols, partitions, schema, table );

        Log.write(jConfig, hiveScript);

        spark.sql(hiveScript);

    }

    /**
     * Создание Hive таблицы из StructType
     *
     * @param jConfig
     * @param type
     * @throws Exception
     */
    public static synchronized void createHiveTable(JobConfiguration jConfig, StructType type) throws Exception {

        createHiveTable(jConfig, LoaderUtils.getFormattingCols(type).toArray(new String[0]));

    }

    /**
     * Создание Hive таблицы из коллекции полей
     *
     * @param jConfig
     * @param listColumns
     * @throws Exception
     */
    public static synchronized void createHiveTable(JobConfiguration jConfig, Set<String> listColumns) throws Exception {
        createHiveTable(jConfig, listColumns.toArray(new String[0]));
    }

}
