package de.uniko.sebschlicht.graphity.neo4j.model;

import java.util.Comparator;

// TODO documentation
public class PostIteratorComparator implements Comparator<PostIterator> {

    @Override
    public int compare(final PostIterator iter1, final PostIterator iter2) {
        if (iter1.getCrrPublished() > iter2.getCrrPublished()) {
            return 1;
        } else {
            return -1;
        }
    }
}
