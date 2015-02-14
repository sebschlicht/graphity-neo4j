package de.uniko.sebschlicht.graphity.neo4j;

import org.neo4j.graphdb.Label;

/**
 * items the social graph node can represent
 * 
 * @author sebschlicht
 * 
 */
public enum NodeType implements Label {

    /**
     * status update displayed in user streams
     */
    UPDATE,

    /**
     * user that can act in the social network
     */
    USER;
}
