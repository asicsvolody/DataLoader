/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 *
 * Класс сгрузки данных из нескольких директорий в одну автоматически сгенерированную таблицу Hive
 * Если в сгружаемой таблице нет поля партицирования генерируется Exception
 * Остальные недостающие поля заполняются Null
 * После успешного выполнения директории "from" удаляются
 */

package ru.yakimov.Jobs;


import org.apache.spark.sql.SparkSession;
import ru.yakimov.BootProcessMain;
import ru.yakimov.MySqlDB.Log;
import ru.yakimov.utils.HdfsUtils;
import ru.yakimov.utils.HiveUtils;
import ru.yakimov.utils.LoaderUtils;
import ru.yakimov.utils.SparkUtils;

import java.util.*;

public class LoadToHiveFromDirs extends Job {
    @Override
    public Integer call() throws Exception {

        SparkSession spark = BootProcessMain.CONTEXT.getBean(SparkSession.class);

        Log.write(jobConfig, "Checking directory to");

        HdfsUtils.deleteDirWithLog(jobConfig, jobConfig.getDirTo());

        Log.write(jobConfig, "Read columns from data");

        Set<String> fullColsSet = new HashSet<>();

        for (String dir : jobConfig.getDirsFrom()) {

            List<String> colsList = SparkUtils.getFormattingColsFromDir(dir, jobConfig);

            Log.write(jobConfig, "Add columns to  colsSet");

            fullColsSet.addAll(colsList);
        }

        Log.write(jobConfig, "Creating hive table");

        HiveUtils.createTransactionalHiveTable(jobConfig, fullColsSet);

        Log.write(jobConfig, "Dynamic insert into hiveTable");

        spark.sql(String.format("describe %s.%s",jobConfig.getDbConfiguration().getSchema(), jobConfig.getDbConfiguration().getTable())).show();



        for (String dir : jobConfig.getDirsFrom()) {

            HiveUtils.insetToHiveTable(jobConfig, dir);

        }

        Log.write(jobConfig, "Delete dirs from");

        LoaderUtils.deleteDirs(jobConfig, jobConfig.getDirsFrom());


        return 0;
    }
}
