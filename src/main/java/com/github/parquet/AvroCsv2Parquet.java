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
 * @datetime 2021-05-28 08:52
 * @description
 */
public class AvroCsv2Parquet {

    private static final String USAGE = "java -cp parquet4j-writer-1.0.0.jar:. com.github.parquet.AvroCsv2Parquet \n" +
            "   -csv csv-file \n" +
            "   -skip-header true|false \n" +
            "   -csv-separator separator \n" +
            "   -output output-file \n" +
            "   -avro avro-file";

    public static void main(String[] args){
        if(args.length != 5){
            System.err.println(USAGE);
            System.exit(-1);
        }

        String csvFile = args[0];
        String skipHeader = args[1];
        String csvSeparator = args[2];
        String outputFile = args[3];
        String avroFile = args[4];

        int skipLines = skipHeader.trim().toLowerCase().equals("true") ? 1 : 0;

        FileInputStream fis;
        InputStreamReader isr;
        CSVReader reader = null;
        char separator = csvSeparator.trim().charAt(0);
        long startTs = System.currentTimeMillis();
        try{
            Schema schema = new Schema.Parser().parse(new File(avroFile));
            LocalDataOutputStream localDataOutputStream = new LocalDataOutputStream(new File(outputFile));
            ParquetWriterFactory<GenericRecord> writerFactory = ParquetAvroWriters.forGenericRecord(schema);
            System.out.println("avro schema:\n" + schema.toString());
            BulkWriter<GenericRecord> bulkWriter = writerFactory.create(localDataOutputStream);

            fis = new FileInputStream(csvFile);
            isr = new InputStreamReader(fis, StandardCharsets.UTF_8);
            CSVParser csvParser = new CSVParserBuilder().withSeparator(separator).build();
            reader = new CSVReaderBuilder(isr).withSkipLines(skipLines).withCSVParser(csvParser).build();

            List<Schema.Field> fields = schema.getFields();

            int totalRecordWritten = 0;
            Iterator<String[]> iterator = reader.iterator();
            while(iterator.hasNext()){
                String[] lineArr = iterator.next();

                GenericRecord record = new GenericData.Record(schema);
                for(int i = 0; i < lineArr.length; i++){
                    String strValue = lineArr[i];
                    Schema.Field field = fields.get(i);
                    String fieldName = field.name();
                    Schema.Type type = field.schema().getType();
                    switch (type){
                        case ENUM:
                            break;
                        case BOOLEAN:
                            boolean boolValue = Boolean.valueOf(strValue);
                            record.put(fieldName,boolValue);
                            break;
                        case INT:
                            int intValue = Integer.valueOf(strValue);
                            record.put(fieldName,intValue);
                            break;
                        case LONG:
                            long longValue = Long.valueOf(strValue);
                            record.put(fieldName,longValue);
                            break;
                        case FLOAT:
                            float floatValue = Float.valueOf(strValue);
                            record.put(fieldName,floatValue);
                            break;
                        case DOUBLE:
                            double doubleValue = Double.valueOf(strValue);
                            record.put(fieldName,doubleValue);
                            break;
                        case STRING:
                            record.put(fieldName,strValue);
                            break;
                        case MAP:
                            break;
                        case NULL:
                            record.put(fieldName,null);
                            break;
                        case ARRAY:
                            break;
                        case BYTES:
                            break;
                        case FIXED:
                            break;
                        case UNION:
                            break;
                        case RECORD:
                            break;
                    }
                }

                totalRecordWritten++;
                if(totalRecordWritten % 1000 == 0){
                    System.out.println("total record written:" + totalRecordWritten);
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

        long totalMillis = System.currentTimeMillis() - startTs;
        StringBuilder sb = new StringBuilder();
        Utils.appendPosixTime(sb,(int)totalMillis);
        System.out.println("total run time: " + sb.toString());
    }
}
