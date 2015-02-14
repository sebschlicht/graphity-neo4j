package de.uniko.sebschlicht.graphity.neo4j.model;

import java.util.Iterator;

public interface PostIterator extends Iterator<StatusUpdateProxy> {

    long getCrrPublished();
}
