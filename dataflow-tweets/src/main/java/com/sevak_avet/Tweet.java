package com.sevak_avet;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Objects;

/**
 * Created by savetisyan: 04.09.2020
 **/
public class Tweet implements Serializable {
    private String author;
    private String place;
    private String coordinates;
    private LocalDateTime createdAt;
    private String text;
    private String source;
    private long id;

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public String getPlace() {
        return place;
    }

    public void setPlace(String place) {
        this.place = place;
    }

    public String getCoordinates() {
        return coordinates;
    }

    public void setCoordinates(String coordinates) {
        this.coordinates = coordinates;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Tweet tweet = (Tweet) o;
        return id == tweet.id &&
                Objects.equals(author, tweet.author) &&
                Objects.equals(place, tweet.place) &&
                Objects.equals(coordinates, tweet.coordinates) &&
                Objects.equals(createdAt, tweet.createdAt) &&
                Objects.equals(text, tweet.text) &&
                Objects.equals(source, tweet.source);
    }

    @Override
    public int hashCode() {
        return Objects.hash(author, place, coordinates, createdAt, text, source, id);
    }
}
