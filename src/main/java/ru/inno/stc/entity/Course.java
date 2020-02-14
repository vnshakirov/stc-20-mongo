package ru.inno.stc.entity;

import org.bson.types.ObjectId;

import java.util.Objects;
import java.util.StringJoiner;

public class Course {

    private ObjectId id;
    private String code;
    private String title;

    public Course() {
        id = new ObjectId();
    }

    public Course(String code, String title) {
        this();
        this.code = code;
        this.title = title;
    }

    public ObjectId getId() {
        return id;
    }

    public void setId(ObjectId id) {
        this.id = id;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", Course.class.getSimpleName() + "[", "]")
                .add("id=" + id)
                .add("code='" + code + "'")
                .add("title='" + title + "'")
                .toString();
    }

    @Override public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Course course = (Course) o;
        return id == course.id &&
                Objects.equals(code, course.code) &&
                Objects.equals(title, course.title);
    }

    @Override public int hashCode() {
        return Objects.hash(id, code, title);
    }
}
