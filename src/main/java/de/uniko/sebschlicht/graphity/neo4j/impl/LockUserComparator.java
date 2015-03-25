package de.uniko.sebschlicht.graphity.neo4j.impl;

import java.util.Comparator;

import de.uniko.sebschlicht.graphity.neo4j.model.UserProxy;

public class LockUserComparator implements Comparator<UserProxy> {

    @Override
    public int compare(UserProxy u1, UserProxy u2) {
        long i1 = Long.valueOf(u1.getIdentifier());
        long i2 = Long.valueOf(u2.getIdentifier());
        if (i1 < i2) {
            return 1;
        } else if (i1 > i2) {
            return -1;
        } else {
            return 0;
        }
    }
}
