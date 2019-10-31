/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 *
 * Метод экспорта sqoop-ом данных в таблицу базы данных
 */

package ru.yakimov.Jobs;

import org.springframework.stereotype.Component;
import ru.yakimov.Assets;
import ru.yakimov.config.DBConfiguration;
import ru.yakimov.MySqlDB.Log;
import ru.yakimov.utils.HdfsUtils;
import ru.yakimov.utils.SqoopUtils;


@Component
public class ExportSqoopDirToDbJob extends Job {

    @Override
    public Integer call() throws Exception {
        Log.write(jobConfig, "Sqoop get task Export");

        if(jobConfig.getDirsFrom().length !=1){
            Log.write(jobConfig, "Wrong dir from array size", Log.Level.ERROR);
            return 1;
        }

        DBConfiguration dbConfig = jobConfig.getDbConfiguration();

        SqoopUtils.createPassword(dbConfig.getPassword(), jobConfig);

        Log.write(jobConfig, "Sqoop run process");

        Process process = Assets.getInstance().getRt().exec(String.format("sqoop export " +
                "--connect \"jdbc:mysql://%s:%s/%s?serverTimezone=UTC&zeroDateTimeBehavior=CONVERT_TO_NULL\" " +
                "--username %s " +
                "--password-file %s " +
                "--table %s " +
                "--export-dir %s " +
                "--validate"
                ,dbConfig.getHost()
                ,dbConfig.getPort()
                ,dbConfig.getSchema()
                ,dbConfig.getUser()
                ,SqoopUtils.getHadoopPasswordPath(jobConfig.getJobName())
                ,dbConfig.getTable()
                ,jobConfig.getDirTo())
        );

        Log.write(jobConfig, "Waiting of end sqoop process");

        process.waitFor();

        SqoopUtils.writeProcessMessageStream(process, jobConfig);

        SqoopUtils.writeResSqoop(jobConfig, process.exitValue());

        if(process.exitValue() == 0){
            Log.write(jobConfig, "Delete dir from");
            HdfsUtils.deleteDirWithLog(jobConfig, jobConfig.getDirsFrom()[0]);
        }

        Log.write(jobConfig, "Delete password: " + SqoopUtils.getHadoopPasswordPath(jobConfig.getJobName() ));

        HdfsUtils.deleteDirWithLog(jobConfig, SqoopUtils.getHadoopPasswordPath(jobConfig.getJobName()));


        return process.exitValue();
    }
}
