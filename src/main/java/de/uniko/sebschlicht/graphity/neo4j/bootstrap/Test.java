package de.uniko.sebschlicht.graphity.neo4j.bootstrap;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import de.uniko.sebschlicht.graphity.exception.UnknownReaderIdException;
import de.uniko.sebschlicht.graphity.neo4j.Neo4jGraphity;
import de.uniko.sebschlicht.graphity.neo4j.impl.WriteOptimizedGraphity;

public class Test {

    public static void main(String[] args) throws UnknownReaderIdException {
        GraphDatabaseService graph =
                new GraphDatabaseFactory()
                        .newEmbeddedDatabase("/media/shared/tmp/neotestdb");
        Neo4jGraphity graphity = new WriteOptimizedGraphity(graph);
        System.out.println("loading indices..");
        graphity.init();
        System.out.println("database ready.");

        int numUsersNonEmpty = 0, feedSize;
        for (int i = 1; i <= 40050; ++i) {
            feedSize = graphity.readStatusUpdates(String.valueOf(i), 15).size();
            if (feedSize > 0) {
                numUsersNonEmpty += 1;
            }
            if (i % 1000 == 0) {
                System.out.println("progress: " + i);
            }
        }
        System.out.println(numUsersNonEmpty
                + " users have news feed to display.");
        int numFeeds = 0;
        for (int i = 1; i <= 40050; ++i) {
            feedSize = graphity.readStatusUpdates(String.valueOf(i), 15).size();
            if (feedSize > 0) {
                numFeeds += feedSize;
            }
            if (i % 1000 == 0) {
                System.out.println("progress: " + i);
            }
        }
        System.out.println(numFeeds + " feeds in total.");
    }
}
