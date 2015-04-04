package de.uniko.sebschlicht.graphity.neo4j.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.Transaction;

import de.uniko.sebschlicht.graphity.neo4j.model.UserProxy;

public class LockManager {

    protected static Map<Transaction, TreeSet<Lock>> _LOCKS = new HashMap<>();

    public static Lock[] lock(Transaction tx, UserProxy user1, UserProxy user2) {
        Lock[] locks = new Lock[2];
        if (user1.getIdentifier() <= user2.getIdentifier()) {
            locks[0] = tx.acquireWriteLock(user1.getNode());
            locks[1] = tx.acquireWriteLock(user2.getNode());
        } else {
            locks[0] = tx.acquireWriteLock(user2.getNode());
            locks[1] = tx.acquireWriteLock(user1.getNode());
        }
        return locks;
    }

    public static void releaseLocks(Lock[] locks) {
        locks[1].release();
        locks[0].release();
    }
}
