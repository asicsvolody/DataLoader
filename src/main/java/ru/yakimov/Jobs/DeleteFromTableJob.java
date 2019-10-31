/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 */

package ru.yakimov.Jobs;

import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Component;
import ru.yakimov.Assets;
import ru.yakimov.MySqlDB.Log;
import ru.yakimov.utils.HiveUtils;
import ru.yakimov.utils.LoaderUtils;

@Component
public class DeleteFromTableJob extends Job {

    @Override
    public Integer call() throws Exception {

        SparkSession spark = Assets.getInstance().getSpark();
        spark.sql(String.format("SELECT * FROM %s.%s",jobConfig.getDbConfiguration().getSchema(),jobConfig.getDbConfiguration().getTable())).sort("user_id").show();

        for (String dir : jobConfig.getDirsFrom()) {
            HiveUtils.deleteFromHiveTable(jobConfig, dir);
        }

        Log.write(jobConfig, "Delete dirs from");

        LoaderUtils.deleteDirs(jobConfig, jobConfig.getDirsFrom());

        spark.sql(String.format("SELECT * FROM %s.%s",jobConfig.getDbConfiguration().getSchema(),jobConfig.getDbConfiguration().getTable())).sort("user_id").show();

        return 0;
    }
}
