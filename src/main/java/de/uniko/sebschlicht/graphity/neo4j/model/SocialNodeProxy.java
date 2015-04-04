package de.uniko.sebschlicht.graphity.neo4j.model;

import org.neo4j.graphdb.Node;

public abstract class SocialNodeProxy {

    protected Node _node;

    public SocialNodeProxy(
            Node node) {
        if (node == null) {
            throw new IllegalStateException(
                    "You must provide a node to create a proxy for it!");
        }
        this._node = node;
    }

    public Node getNode() {
        return _node;
    }
}
