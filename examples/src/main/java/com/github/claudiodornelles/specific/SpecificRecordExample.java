package com.github.claudiodornelles.specific;

import com.github.claudiodornelles.avro.Customer;
import java.io.File;
import java.io.IOException;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpecificRecordExample {

  private static final Logger LOGGER = LoggerFactory.getLogger(SpecificRecordExample.class.getSimpleName());

  public static void main(String[] args) {
    final var customerBuilder = Customer.newBuilder();
    customerBuilder.setAge(25);
    customerBuilder.setFirstName("John");
    customerBuilder.setLastName("Moe");
    customerBuilder.setHeight(175.8F);
    customerBuilder.setWeight(93.4F);
    customerBuilder.setMiddleName("Mike");
    customerBuilder.setAutomatedEmail(false);
    final var firstCustomer = customerBuilder.build();
    LOGGER.info("firstCustomer: {}", firstCustomer);

    final var secondCustomerBuilder = Customer.newBuilder();
    secondCustomerBuilder.setAge(25);
    secondCustomerBuilder.setFirstName("John");
    secondCustomerBuilder.setLastName("Moe");
    secondCustomerBuilder.setHeight(175.8F);
    secondCustomerBuilder.setWeight(93.4F);
    final var secondCustomerWithDefaultValues = secondCustomerBuilder.build();
    LOGGER.info("secondCustomerWithDefaultValues: {}", secondCustomerWithDefaultValues);

    // Serialize customers to disk
    final var datumWriter = new SpecificDatumWriter<>(Customer.class);
    final var file = new File("customer-specific.avro");

    try (final var dataFileWriter = new DataFileWriter<>(datumWriter)){
      dataFileWriter.create(Customer.getClassSchema(), file);
      dataFileWriter.append(firstCustomer);
      dataFileWriter.append(secondCustomerWithDefaultValues);
      LOGGER.info("Data wrote to customer-specific.avro");
    } catch (IOException e) {
      LOGGER.error("Could not write data to file. {}", e.getMessage(), e);
    }

    // Deserialize customers from disk
    final var datumReader = new SpecificDatumReader<>(Customer.class);
    try {
      try (var dataFileReader = new DataFileReader<>(file, datumReader)) {
        Customer customer = null;
        while (dataFileReader.hasNext()) {
          customer = dataFileReader.next(customer);
          LOGGER.info("Read customer from file: {}", customer);
          LOGGER.info("customer.getSchema(): {}", customer.getSchema());
          LOGGER.info("customer.getFirstName(): {}", customer.getFirstName());
        }
      }
    } catch (IOException e) {
      LOGGER.error("Could not read schema from file. {}", e.getMessage(), e);
    }

  }

}