package de.uniko.sebschlicht.graphity.neo4j.impl;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeSet;

import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.Transaction;

import de.uniko.sebschlicht.graphity.neo4j.model.UserProxy;

public class LockManager {

    protected static Map<Transaction, LinkedList<Lock>> _LOCKS =
            new HashMap<>();

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

    public static void lock(Transaction tx, Collection<UserProxy> users) {
        TreeSet<UserProxy> userSet = new TreeSet<>(new LockUserComparator());
        for (UserProxy user : users) {
            userSet.add(user);
        }

        LinkedList<Lock> locks = _LOCKS.get(tx);
        if (locks == null) {
            locks = new LinkedList<>();
            _LOCKS.put(tx, locks);
        }
        for (UserProxy user : userSet) {
            Lock lock = tx.acquireWriteLock(user.getNode());
            locks.addLast(lock);
        }
    }

    public static void releaseLocks(Lock[] locks) {
        locks[1].release();
        locks[0].release();
    }

    public static void releaseLocks(Transaction tx) {
        LinkedList<Lock> locks = _LOCKS.remove(tx);
        if (locks == null) {
            return;
        }
        for (Lock lock : locks) {
            lock.release();
        }
        locks.clear();
        locks = null;
    }
}
