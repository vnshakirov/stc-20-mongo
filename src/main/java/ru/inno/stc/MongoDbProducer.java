package ru.inno.stc;

import com.google.gson.Gson;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import ru.inno.stc.entity.Course;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;

public class MongoDbProducer {
    private MongoClient client;
    private MongoDatabase database;

    public void init() {
        //client = MongoClients.create("mongodb://admin2:1234@localhost:27017/?authSource=STC-20");
        //CodecRegistry registry = CodecRegistries.fromRegistries(MongoClientSettings.getDefaultCodecRegistry(), CodecRegistries.fromProviders(PojoCodecProvider.builder().automatic(true).build()));
        client = MongoClients.create("mongodb://localhost:27017");
        database = client.getDatabase("STC-20");
        //database = database.withCodecRegistry(registry);
    }

    public void close() {
        client.close();
    }

    public void addCourse(Course course) {
        MongoCollection collection = database.getCollection("Course");
        Document document = new Document();
        document.put("id", course.getId());
        document.put("title", course.getTitle());
        collection.insertOne(document);
    }

    public List<Course> getCourses() {
        List<Course> result = new LinkedList<>();
        MongoCollection collection = database.getCollection("Course");
        MongoCursor cursor = collection.find().iterator();
        while (cursor.hasNext()) {
            Document document = (Document) cursor.next();
            Course course = new Course();
            //course.setId((Integer) document.get("id"));
            course.setTitle((String) document.get("title"));
            result.add(course);
        }
        return result;
    }

    public void addObject(Object object, String collectionName) {
        MongoCollection collection = database.getCollection(collectionName);
        Document document = Document.parse(new Gson().toJson(object));
        collection.insertOne(document);
    }

    public List<Object> getObjectList(String collectionName, Class<?> objectClass) {
        List<Object> result = new LinkedList<>();
        MongoCollection collection = database.getCollection(collectionName);
        MongoCursor cursor = collection.find().iterator();
        while (cursor.hasNext()) {
            Document document = (Document) cursor.next();
            Object object = new Gson().fromJson(document.toJson(), objectClass);
            result.add(object);
        }
        return result;
    }

    public List<Course> getCourses(String title) {
        List<Course> result = new LinkedList<>();
        MongoCollection collection = database.getCollection("Course");
        Document query = new Document();
        query.put("title", title);
        MongoCursor cursor = collection.find(query).iterator();
        while (cursor.hasNext()) {
            Document document = (Document) cursor.next();
            Course object = new Gson().fromJson(document.toJson(), Course.class);
            result.add(object);
        }
        return result;
    }

    public List<Course> getCourses(int id) {
        List<Course> result = new LinkedList<>();
        MongoCollection collection = database.getCollection("Course");
        MongoCursor cursor = collection.find(Filters.and(Filters.gte("id", id), Filters.eq("title", "The Pinball Masterclass"))).iterator();
        while (cursor.hasNext()) {
            Document document = (Document) cursor.next();
            Course object = new Gson().fromJson(document.toJson(), Course.class);
            result.add(object);
        }
        return result;
    }

    public void updateCourse(String fieldName, Object oldVal, Object newVal) {
        MongoCollection collection = database.getCollection("Courses");
        Document oldDoc = new Document();
        oldDoc.put(fieldName, oldVal);
        collection.updateOne(oldDoc, Updates.set(fieldName, newVal));
    }

    public void insertCourse(Course course) {
        MongoCollection<Course> collection = database.getCollection("Courses", Course.class);
        collection.insertOne(course);
    }

    public List<Course> findCourses() {
        List<Course> courses = new LinkedList<>();
        MongoCollection<Course> collection = database.getCollection("Courses", Course.class);
        Document document = new Document();
        document.put("title", 1);
        MongoCursor<Course> cursor = collection.find().filter(document).iterator();
        while (cursor.hasNext()) {
            courses.add(cursor.next());
        }
        return courses;
    }

    public void deleteCourse(String fieldName, Object value) {
        MongoCollection<Course> collection = database.getCollection("Courses", Course.class);
        Document document = new Document();
        document.put(fieldName, value);
        collection.deleteOne(document);
    }

    public void createIndex(String fieldName) {
        MongoCollection<Course> collection = database.getCollection("Courses", Course.class);
        collection.createIndex(Indexes.descending(fieldName));
    }

    public void deleteCourseByFilter(String fieldName, Object value) {
        MongoCollection<Course> collection = database.getCollection("Courses", Course.class);
        collection.deleteOne(Filters.eq(fieldName, value));
    }

    public void mapReduce() {
        MongoCollection collection = database.getCollection("Courses");
        String map = "function () {if (this.title == 'The Pinball Masterclass') {emit('count', 1);}}";
        String reduce = "function(key, value) { var total = 0; for (var i = 0; i < value.length; i++) {" +
                "total += value[i];}" +
                "return total;}";
        collection.mapReduce(map, reduce).forEach((Consumer)System.out::println);
    }
}
