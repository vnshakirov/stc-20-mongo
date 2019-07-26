package ru.inno.stc;

import ru.inno.stc.entity.Course;

import java.util.ArrayList;
import java.util.List;

public class App {

  public static void main(String[] args) {

    MongoDbProducer producer = new MongoDbProducer();
    producer.init();


    // create some courses
    List<Course> courses = new ArrayList<>();
    Course tempCourse1 = new Course("AG-1", "Air Guitar - The Ultimate Guide");
    courses.add(tempCourse1);
    Course tempCourse2 = new Course("PM-1", "The Pinball Masterclass");
    courses.add(tempCourse2);
    producer.insertCourse(tempCourse1);
    producer.insertCourse(tempCourse2);
    //producer.addCourse(tempCourse1);

    //instructor.setCourses(courses);
    //producer.addObject(instructor, "Instructor");


    System.out.println("==========================================");
    //producer.getObjectList("Instructor", Instructor.class);
    System.out.println("================DELETE=================");
    //producer.deleteCourse("title", "Air Guitar - The Ultimate Guide");
    producer.findCourses().forEach(System.out::println);
    producer.mapReduce();
    System.out.println("================UPDATE=================");
    producer.updateCourse("title", "The Pinball Masterclass", "The Pinball Masterclass2");
    producer.findCourses().forEach(System.out::println);

    //producer.deleteCourseByFilter("title", "Air Guitar - The Ultimate Guide");
    //producer.findCourses().forEach(System.out::println);
    System.out.println("==========================================");
    //producer.getCourses().forEach(System.out::println);
    System.out.println("==========================================");
    //producer.getObjectList("Instructor", Instructor.class).forEach(System.out::println);
    System.out.println("==========================================");
    //producer.getCourses("The Pinball Masterclass").forEach(System.out::println);
    System.out.println("===================ID===================");
    //producer.updateCourse("title", "The Pinball Masterclass", 2);
    System.out.println("==========================================");
    //producer.getCourses(1).forEach(System.out::println);
    System.out.println("==========================================");
    //producer.getCourses(2).forEach(System.out::println);
    producer.close();
  }
}
