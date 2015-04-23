package de.uniko.sebschlicht.graphity.neo4j.impl;

import java.util.Comparator;

public class LockUserComparator implements Comparator<LockableUser> {

    @Override
    public int compare(LockableUser u1, LockableUser u2) {
        long i1 = u1.getIdentifier();
        long i2 = u2.getIdentifier();
        if (i1 < i2) {
            return -1;
        } else if (i1 > i2) {
            return 1;
        } else {
            return 0;
        }
    }
}
