package de.uniko.sebschlicht.graphity.neo4j;

import org.neo4j.graphdb.RelationshipType;

/**
 * relations the social graph edge can represent
 * 
 * @author sebschlicht
 * 
 */
public enum EdgeType implements RelationshipType {

    /**
     * one user follows another one
     */
    FOLLOWS,

    /**
     * news feed item published
     */
    PUBLISHED,

    /**
     * Graphity index spanning user's ego network
     */
    GRAPHITY,

    /**
     * replication nodes to overcome relationship type limitation
     */
    REPLICA;
}
