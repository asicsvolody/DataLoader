/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 */

package ru.yakimov;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import ru.yakimov.Jobs.ImportSqoopDbToDirJob;
import ru.yakimov.Jobs.PartitionSparkDataJob;

@Configuration
public class JobContextConfiguration {

    @Bean
    @Scope("prototype")
    public PartitionSparkDataJob loadSparkPartitionTableJob(){
        return new PartitionSparkDataJob();
    }

    @Bean
    @Scope("prototype")
    public ImportSqoopDbToDirJob loadImportSqoopDbToDirJob(){
        return new ImportSqoopDbToDirJob();
    }




}
