/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 *
 * Класс SPRING BEAN
 */

package ru.yakimov;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import ru.yakimov.Jobs.ImportSqoopDbToDirJob;
import ru.yakimov.Jobs.LoadToHiveFromDirs;

@Configuration
public class JobContextConfiguration {

    @Bean
    @Scope("prototype")
    public ImportSqoopDbToDirJob loadImportSqoopDbToDirJob(){
        return new ImportSqoopDbToDirJob();
    }

    @Bean
    @Scope("prototype")
    public LoadToHiveFromDirs loadJoinAnyBaseInOne(){
        return new LoadToHiveFromDirs();
    }




}
