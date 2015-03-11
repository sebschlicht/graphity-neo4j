package de.uniko.sebschlicht.graphity.neo4j.model;

import org.neo4j.graphdb.Node;

public abstract class SocialNodeProxy {

    protected Node node;

    public SocialNodeProxy(
            Node node) {
        if (node == null) {
            throw new IllegalStateException(
                    "You must provide a node to create a proxy for it!");
        }
        this.node = node;
    }

    public Node getNode() {
        return node;
    }
}
