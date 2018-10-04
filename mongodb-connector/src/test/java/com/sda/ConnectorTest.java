package com.sda;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonWriterSettings;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import static com.mongodb.client.model.Accumulators.avg;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.*;

public class ConnectorTest {

    //LOG4J, SLF4J
    //LEVELS : INFO, DEBUG, WARN, TRACE, ERROR
    private static final Logger LOGGER = Logger.getLogger("ConnectorTest");

    //given
    Connector connector = new Connector();

    public ConnectorTest() throws IOException {
    }

    @Test
    public void connect() {
        //when
        MongoDatabase db = connector.connect();
        //then
        Assert.assertEquals(db.getName(), "test");
        System.out.println(db.getCollection("grades").count());
    }

    @Test
    public void addDocumentToDB() {
        //{ “_id” : ObjectId(“50906d7fa3c412bb040eb57a”),
        // “student_id” : 0, “type” : “homework”, “score” : 63.98402553675503 }
        Document document = new Document("student_id", 99999);

        //when
        MongoDatabase db = connector.connect();
        db.getCollection("grades").insertOne(document);

    }

    @Test
    public void findAllAndPrintThemOnTheConsole() {
        MongoDatabase db = connector.connect();
        //when
        for (Document grade : db.getCollection("grades").find()) {
            System.out.println(grade);
        }
        //then
        //this test sucks. It needs to have assertion!

    }

    @Test
    public void findAllStudentsWhereStudentIdEquals197() {
        MongoDatabase db = connector.connect();
        //when
        Document filter = new Document("student_id", 197);
        FindIterable<Document> grades = db.getCollection("grades").find(filter);
        for (Document grade : grades) {
            System.out.println(grade);
        }
    }

    @Test
    public void findAllStudentsWhereStudentIdEquals197Or198() {
        MongoDatabase db = connector.connect();
        //when
//        Document filter = new Document("student_id", 197);
//        Document filter2 = new Document("student_id", 198);
//        Bson filters = or(filter, filter2);

        Bson filters = or(
                eq("student_id", 197),
                eq("student_id", 198)
        );

        FindIterable<Document> grades = db.getCollection("grades").find(filters);
        for (Document grade : grades) {
            System.out.println(grade);
        }
    }

    @Test
    public void findAllStudentsWhereStudentIdEquals197Or198AndTypeOfScoreIsExam() {
        MongoDatabase db = connector.connect();
        //when
        Bson filters = and(
                or(
                        eq("student_id", 197),
                        eq("student_id", 198)
                ),
                eq("type", "exam")
        );
        FindIterable<Document> grades = db.getCollection("grades").find(filters);

        for (Document grade : grades) {
            System.out.println(grade);
        }
    }

    @Test
    public void findAllStudentsWhereStudentIdEquals197Or198AndTypeOfScoreIsExamOrScoreIsGreaterThan50() {

        MongoDatabase db = connector.connect();
        //when
        Bson filters = and(
                or(
                        eq("student_id", 198),
                        eq("student_id", 197)
                ),
                or(
                        eq("type", "exam"),
                        gt("score", 49)
                )

        );
        FindIterable<Document> grades = db.getCollection("grades").find(filters);

        for (Document grade : grades) {
            System.out.println(grade);
        }
    }

    /**
     * * b.grades.aggregate([{
     * $group: {
     * _id: “$student_id”,
     * average: {
     * $avg: “$score”
     * }
     * }
     * }])
     */
    @Test
    public void findAvgScoresPerEachStudent() {
        MongoDatabase db = connector.connect();
        //when

        Bson match = match(new Document());
        Bson group = group("$student_id", avg("average", "$score"));


        List<Bson> bsons = new ArrayList<>();
        bsons.add(match);
        bsons.add(group);
        AggregateIterable<Document> grades = db.getCollection("grades").aggregate(bsons);

        for (Document grade : grades) {
            System.out.println(
                    grade.toJson(
                            JsonWriterSettings.
                                    builder().
                                    indent(true). // here we have a nice json 🌈
                                    build()
                    )
            );
        }
    }

    /**
     * * b.grades.aggregate([{
     * $group: {
     * _id: “$student_id”,
     * average: {
     * $avg: “$score”
     * }
     * }
     * }])
     */
    @Test
    public void findAvgScoresPerEachStudentWithOrder() {
        MongoDatabase db = connector.connect();
        //when

        Bson match = match(new Document());
        Bson group = group("$student_id", avg("average", "$score"));
        Bson sort = sort(new Document("average", -1));

        List<Bson> bsons = new ArrayList<>();
        bsons.add(match);
        bsons.add(group);
        bsons.add(sort);
        AggregateIterable<Document> grades = db.getCollection("grades").aggregate(bsons);

        for (Document grade : grades) {
            System.out.println(
                    grade.toJson(
                            JsonWriterSettings.
                                    builder().
                                    indent(true). // here we have a nice json 🌈
                                    build()
                    )
            );
        }
    }
}