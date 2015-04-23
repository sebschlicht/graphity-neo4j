package de.uniko.sebschlicht.graphity.neo4j.impl;

public class SimpleUser implements LockableUser {

    protected long _identifier;

    public SimpleUser(
            long identifier) {
        _identifier = identifier;
    }

    @Override
    public long getIdentifier() {
        return _identifier;
    }
}
