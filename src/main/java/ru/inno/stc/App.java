package ru.inno.stc;

import ru.inno.stc.entity.Course;

import java.util.ArrayList;
import java.util.List;

public class App {

  public static void main(String[] args) {

    MongoDbProducer producer = new MongoDbProducer();
    producer.init();

    // create some courses
    Course tempCourse1 = new Course("AG-2", "Air Guitar - The Ultimate Guide");
    Course tempCourse2 = new Course("PM-3", "The Pinball Masterclass");
    //producer.insertCourseNew(tempCourse2);
    //producer.insertCourse(tempCourse2);
    //Course newCourse = new Course("AG-1", "Air Guitar - The Ultimate Guide-1");
    //producer.updateCourse(tempCourse1, newCourse);
    /*producer.getCoursesJson().forEach(c -> {
      System.out.println(c.toString());
    });*/
    producer.getCoursesNew().forEach(c -> {
      System.out.println(c.toString());
    });
    System.out.println("--------------------");

    producer.createIndex();
    producer.getCoursesNew().forEach(c -> {
      System.out.println(c.toString());
    });
    //producer.mapReduce();
    //producer.updateCourse(tempCourse1, newCourse);
    /*producer.deleteCourse(tempCourse1);
    producer.getCourses().forEach(c -> {
      System.out.println(c.toString());
    });

    producer.deleteCourse(newCourse);
    producer.getCourses().forEach(c -> {
      System.out.println(c.toString());
    });*/

    producer.close();
  }
}
