/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 *
 * Главный класс программы с main методом
 * Осуществляет запуск конфигурации задач и запуск корневый задач к исполнению
 */

package ru.yakimov;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import ru.yakimov.Jobs.RootJob;
import ru.yakimov.LogDB.Log;
import ru.yakimov.LogDB.LogsFileWriter;
import ru.yakimov.config.AppConfiguration;
import ru.yakimov.config.JobXmlLoader;
import ru.yakimov.utils.RootJobsCreator;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

public class BootProcessMain {

    public static final ApplicationContext CONTEXT = new AnnotationConfigApplicationContext(JobContextConfiguration.class);

    public static final String SEPARATOR = "/";
    public final static String CONF_FILE_PATH = "conf.xml";
    public static final String MAIN_PROS = JobXmlLoader.createNameWithData("SYSTEM_PROSES");

    private RootJob[] rootJobs;


    public BootProcessMain() {

        try {

            AppConfiguration appConfig = CONTEXT.getBean(AppConfiguration.class);
            rootJobs = RootJobsCreator.getRootJobsFromDir(appConfig.getJobsDir());


            if (rootJobs == null) {
                Log.writeRoot(MAIN_PROS, "Jobs not found");
                return;
            }

            ExecutorService service = Executors.newCachedThreadPool();

            Map<String, Future<Integer>> futureMap = new HashMap<>();

            Log.writeRoot(MAIN_PROS, "ExecutorService is ready");


            for (RootJob rootJob : rootJobs) {
                futureMap.put(rootJob.getRootJobName(), service.submit(rootJob));

                Log.writeRoot(MAIN_PROS, rootJob.getRootJobName()+"have been run");
            }

            Log.writeRoot(MAIN_PROS, "All jobs have been run");

            service.shutdown();

            service.awaitTermination(10, TimeUnit.HOURS);


            for (String rootJobName : futureMap.keySet()) {
                printFutureResults(rootJobName, futureMap.get(rootJobName));
            }
            Log.writeRoot(MAIN_PROS, "BootProsesMain has finished.");

            Log.writeRoot(MAIN_PROS, "Write logs files.");

            Log.writeRoot(MAIN_PROS, "Finish!!!");




        }catch (SQLException e){
            e.printStackTrace();
        }
        catch (Exception e){
            e.printStackTrace();
            Log.writeSysException(MAIN_PROS, e);
        }
        finally {

            try {
                if (rootJobs != null) {
                    for (RootJob rootJob : rootJobs) {
                        LogsFileWriter.writeRootLog(rootJob.getRootJobName());
                    }
                }
                LogsFileWriter.writeRootLog(MAIN_PROS);
            } catch (Exception e) {
                System.out.println("Logs not write");
                e.printStackTrace();
            }

            Log.closeConnection();
        }


    }

    /**
     * Метод записи результатов Future
     * @param rootJobName
     * @param future
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws SQLException
     */
    private static void printFutureResults(String rootJobName, Future<Integer> future) throws ExecutionException, InterruptedException, SQLException {
        StringBuilder str = new StringBuilder(rootJobName);

        switch (future.get()) {
            case 0:
                str.append(" completed successfully.");
                break;
            case 1:
                str.append(" completed with error");
                break;
        }

        System.out.println(str.toString());
        Log.writeRoot(MAIN_PROS, str.toString());
    }

    public static void main(String[] args) {
        new BootProcessMain();
    }
}
