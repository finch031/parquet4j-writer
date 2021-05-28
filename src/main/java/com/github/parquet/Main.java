package com.github.parquet;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

/**
 * @author yusheng
 * @version 1.0.0
 * @datetime 2021-05-26 15:25
 * @description
 */
public class Main {

    public static void main(String[] args){
        String csvFile = "D:\\temp\\20210527\\02.csv";
        String avroFile = "D:\\yusheng\\code\\github\\my-github\\parquet4j-writer\\src\\main\\resources\\weather.avro";
        String outputFile = "D:\\temp\\20210526\\out\\weather.parquet";
        char separator = ',';

        FileInputStream fis;
        InputStreamReader isr;
        CSVReader reader = null;
        try{
            Schema schema = new Schema.Parser().parse(new File(avroFile));
            LocalDataOutputStream localDataOutputStream = new LocalDataOutputStream(new File(outputFile));
            ParquetWriterFactory<GenericRecord> writerFactory = ParquetAvroWriters.forGenericRecord(schema);
            System.out.println(schema.toString());
            BulkWriter<GenericRecord> bulkWriter = writerFactory.create(localDataOutputStream);

            fis = new FileInputStream(csvFile);
            isr = new InputStreamReader(fis, StandardCharsets.UTF_8);
            CSVParser csvParser = new CSVParserBuilder().withSeparator(separator).build();
            reader = new CSVReaderBuilder(isr).withSkipLines(1).withCSVParser(csvParser).build();

            List<Schema.Field> fields = schema.getFields();

            int hasWrite = 0;
            Iterator<String[]> iterator = reader.iterator();
            while(iterator.hasNext()){
                String[] lineArr = iterator.next();

                GenericRecord record = new GenericData.Record(schema);
                for(int i = 0; i < lineArr.length; i++){
                    String strValue = lineArr[i];
                    Schema.Field field = fields.get(i);
                    String fieldName = field.name();
                    String fieldType = field.schema().getType().getName().toLowerCase();
                    // System.out.println(fieldName + "," + fieldType + "," + strValue);

                    switch (fieldType){
                        case "string":
                            record.put(fieldName,strValue);
                            break;
                        case "int":
                            int intValue = Integer.parseInt(strValue);
                            record.put(fieldName,intValue);
                            break;
                    }
                }

                hasWrite++;

                if(hasWrite % 10000 == 0){
                    System.out.println("has write:" + hasWrite);
                }

                bulkWriter.addElement(record);
            }

            bulkWriter.finish();
            bulkWriter.flush();
        }catch (IOException ioe){
            ioe.printStackTrace();
        }finally {
            if(reader != null){
                try{
                    reader.close();
                }catch (IOException ioe){
                    ioe.printStackTrace();
                }
            }
        }
    }

}
