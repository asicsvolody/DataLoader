/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 */

package ru.yakimov.config;

import ru.yakimov.Assets;
import ru.yakimov.MySqlDB.Log;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.util.Iterator;

public class JobXmlLoader {

    private static final String ROOT_JOB = "rootJob";
    private static final String JOB = "job";

    private static final String JOB_CLASS = "jobClass";
    private static final String STAGE = "stage";
    private static final String JOB_DIR_FROM = "jobDirFrom";
    private static final String JOB_DIR_TO = "jobDirTo";
    private static final String PARTITION = "partition";

    private static final String DB_CONF = "dbConf";


    public static RootJobConfiguration readConfJob(File file){
        RootJobConfiguration resConfig = null;
        try{
            XMLInputFactory inputFactory = XMLInputFactory.newInstance();
            InputStream in = new FileInputStream(file);
            XMLEventReader eventReader = inputFactory.createXMLEventReader(in);
            RootJobConfiguration config = null;
            JobConfiguration jobConfig = null;
            while(eventReader.hasNext()){
                XMLEvent event = eventReader.nextEvent();
                if(event.isStartElement()){
                    StartElement startElementConf = event.asStartElement();
                    if (startElementConf.getName().getLocalPart().equals(ROOT_JOB)) {
                        config = new RootJobConfiguration(createJobNameFromPath(file));
                    }
                    if (event.isStartElement()) {
                        if (startElementConf.getName().getLocalPart().equals(JOB) && config != null) {
                            jobConfig = createJobConfig(config.getRootJobName(), startElementConf);
                        }
                    }
                    if(event.isStartElement()){
                        if (startElementConf.getName().getLocalPart().equals(DB_CONF) && jobConfig != null) {
                            jobConfig.setDbConfiguration(createDbConfig(startElementConf));
                        }
                    }


                }
                if(event.isEndElement()){
                    EndElement endElement = event.asEndElement();
                    if(endElement.getName().getLocalPart().equals(JOB) && jobConfig != null ){
                        config.addJobConfiguration(jobConfig);
                        jobConfig = null;
                    }
                    if(endElement.getName().getLocalPart().equals(ROOT_JOB)){
                        if(config != null && config.getRootJobName() != null) {
                            resConfig = config;
                            Log.writeRoot(Assets.MAIN_PROS, "Configuration have gotten for " + resConfig.getRootJobName());
                        }
                        break;
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            Log.writeSysException(Assets.MAIN_PROS, e);
        }
        return resConfig;
    }

    @SuppressWarnings("unchecked")
    private static JobConfiguration  createJobConfig(String rootJobName, StartElement startElementConf) {
        JobConfiguration jobConf = new JobConfiguration(rootJobName);
        Iterator<Attribute> attributes = startElementConf.getAttributes();
        while (attributes.hasNext()) {
            Attribute attribute = attributes.next();

            if (attribute.getName().toString().equals(JOB_CLASS)) {
                jobConf.setJobClass(attribute.getValue());
                jobConf.setJobName(createJobNameFromClass(attribute.getValue()));
            }
            if (attribute.getName().toString().equals(STAGE)) {
                jobConf.setStage(Integer.parseInt(attribute.getValue()));
            }
            if (attribute.getName().toString().equals(JOB_DIR_FROM)) {
                jobConf.setDirFrom(attribute.getValue());
            }
            if (attribute.getName().toString().equals(JOB_DIR_TO)) {
                jobConf.setDirTo(attribute.getValue());
            }
            if (attribute.getName().toString().equals(PARTITION)) {
                jobConf.addPartitions(attribute.getValue());
            }
        }
        return jobConf;
    }


    public static DBConfiguration createDbConfig(StartElement startElementConf) {

        DBConfiguration dbConfig = new DBConfiguration();

        AppXmlLoader.setParamsTable(startElementConf, dbConfig);

        return dbConfig;

    }

    private static String createJobNameFromPath(File file){
        String fileName = file.getName().split("\\.")[0];
        return createNameWithData(fileName);
    }

    public static String createNameWithData(String ame){
        String data = getFullTime();
        return ame+ "_" + data;

    }

    public static String getDataNumbersOnly(String string){
        StringBuilder sb = new StringBuilder();
        for (char c : string.toCharArray()) {
            if(c >= '0' && c<='9'){
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private static String createJobNameFromClass(String classPath){
        String[] jobClassArr = classPath.split("\\.");
        String className = jobClassArr[jobClassArr.length-1];
        String data = getFullTime();
        return className + "_" + data;
    }

    private static String getFullTime(){
        return getDataNumbersOnly(LocalDateTime.now().toString());
    }











}
