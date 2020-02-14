package ru.inno.stc;

import com.google.gson.Gson;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import ru.inno.stc.entity.Course;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;

public class MongoDbProducer {
    private MongoClient client;
    private MongoDatabase database;

    public void init() {
        client = MongoClients.create("mongodb://admin2:1234@localhost:27017/?authSource=STC-16");
        database = client.getDatabase("STC-16");
        CodecRegistry registry = CodecRegistries.fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
                CodecRegistries.fromProviders(PojoCodecProvider.builder().automatic(true).build()));
        database = database.withCodecRegistry(registry);
    }

    public void close() {
        client.close();
    }

    public void insertCourse(Course course) {
        MongoCollection collection = database.getCollection("Courses-stc-20-2");
        Document document = Document.parse(new Gson().toJson(course));
        collection.insertOne(document);
    }

    public List<Course> getCourses() {
        MongoCollection collection = database.getCollection("Courses-stc-20-2");
        MongoCursor iterator = collection.find().iterator();
        List<Course> courses = new ArrayList<>();
        while (iterator.hasNext()) {
            Document document = (Document) iterator.next();
            Course course = new Gson().fromJson(document.toJson(), Course.class);
            courses.add(course);
        }
        return courses;
    }

    public List<String> getCoursesJson() {
        MongoCollection collection = database.getCollection("Courses-stc-20-2");
        MongoCursor iterator = collection.find().iterator();
        List<String> courses = new ArrayList<>();
        while (iterator.hasNext()) {
            Document document = (Document) iterator.next();
            courses.add(document.toJson());
        }
        return courses;
    }

    public void updateCourse(Course oldValue, Course newValue) {
        MongoCollection collection = database.getCollection("Courses-stc-20-2");
        Document oldDoc = new Document();
        oldDoc.put("code", oldValue.getCode());
        //collection.updateOne(oldDoc, Updates.set("title", newValue.getTitle()));
        collection.updateOne(oldDoc, Updates.set("title2", newValue.getTitle()));
    }

    public void deleteCourse(Course deleted) {
        MongoCollection collection = database.getCollection("Courses-stc-20-2");
        Document oldDoc = new Document();
        oldDoc.put("code", deleted.getCode());
        oldDoc.put("title", deleted.getTitle());
        collection.deleteOne(oldDoc);
    }

    public void insertCourseNew(Course course) {
        MongoCollection<Course> collection = database.getCollection("Courses-stc-20-2", Course.class);
        collection.insertOne(course);
    }

    public List<Course> getCoursesNew() {
        MongoCollection<Course> collection = database.getCollection("Courses-stc-20-2", Course.class);
        MongoCursor<Course> iterator = collection.find().iterator();
        List<Course> courses = new ArrayList<>();
        while (iterator.hasNext()) {
            courses.add(iterator.next());
        }
        return courses;
    }

    public List<Course> getCoursesByCode(String code) {
        MongoCollection<Course> collection = database.getCollection("Courses-stc-20-2", Course.class);
        Document search = new Document();
        search.put("code", code);
        MongoCursor<Course> cursor = collection.find(search).iterator();
        List<Course> courses = new ArrayList<>();
        while (cursor.hasNext()) {
            courses.add(cursor.next());
        }
        return courses;
    }

    public void mapReduce() {
        MongoCollection collection = database.getCollection("Courses-stc-20-2");
        String map = "function () {emit(this.title, 1);}";
        String reduce = "function (key, value) { var total = 0; for (var i = 0; i < value.length; i++) {"
                + "total += value[i];"
                + "}"
                + "return total;}";
        collection.mapReduce(map, reduce).forEach((Consumer) System.out::println);
    }

    public void createIndex() {
        MongoCollection<Course> collection = database.getCollection("Courses-stc-20-2", Course.class);
        collection.dropIndex(Indexes.ascending("code", "title"));
        collection.createIndex(Indexes.descending("code", "title"));
    }
}
