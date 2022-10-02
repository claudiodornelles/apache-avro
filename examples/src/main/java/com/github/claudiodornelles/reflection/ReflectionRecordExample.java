package com.github.claudiodornelles.reflection;

import java.io.File;
import java.io.IOException;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReflectionRecordExample {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReflectionRecordExample.class.getSimpleName());

  public static void main(String[] args) {
    final var customerSchema = ReflectData.get().getSchema(ReflectionCustomer.class);
    final var schema = customerSchema.toString(true);
    LOGGER.info("Schema = {}", schema);

    final var firstCustomer = new ReflectionCustomer("First name", "Last name", 32);

    final var file = new File("customer-reflection.avro");
    final var datumWriter = new ReflectDatumWriter<>(ReflectionCustomer.class);

    try (var dataFileWriter = new DataFileWriter<>(datumWriter)
        .setCodec(CodecFactory.deflateCodec(9))
        .create(customerSchema, file)) {
      dataFileWriter.append(firstCustomer);
    } catch (IOException e) {
      LOGGER.error("Could not write data to file. {}", e.getMessage(), e);
    }

    final var reader = new ReflectDatumReader<>(ReflectionCustomer.class);

    try (var dataFileReader = new DataFileReader<>(file, reader)) {
      for (var customer : dataFileReader) {
        LOGGER.info("Read customer from file: {}", customer);
      }
    } catch (IOException e) {
      LOGGER.error("Could not read schema from file. {}", e.getMessage(), e);
    }
  }

}
