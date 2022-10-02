package com.github.claudiodornelles.reflection;

import org.apache.avro.reflect.Nullable;

public class ReflectionCustomer {

  private String firstName;
  private String lastName;
  @Nullable private int age;

  public ReflectionCustomer() {
    // Needed for reflection
  }

  public ReflectionCustomer(String firstName, String lastName, int age) {
    this.firstName = firstName;
    this.lastName = lastName;
    this.age = age;
  }

  public String getFirstName() {
    return firstName;
  }

  public void setFirstName(String firstName) {
    this.firstName = firstName;
  }

  public String getLastName() {
    return lastName;
  }

  public void setLastName(String lastName) {
    this.lastName = lastName;
  }

  public int getAge() {
    return age;
  }

  public void setAge(int age) {
    this.age = age;
  }

  @Override
  public String toString() {
    return "ReflectionCustomer{" +
        "firstName='" + firstName + '\'' +
        ", lastName='" + lastName + '\'' +
        ", age=" + age +
        '}';
  }
}
