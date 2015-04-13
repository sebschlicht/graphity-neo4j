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

    /**
     * holds open locks of a transaction
     */
    protected static Map<Transaction, LinkedList<Lock>> _LOCKS =
            new HashMap<>();

    /**
     * Locks two users.<br>
     * What actually is locked, is the user node.<br>
     * The locks returned can be released via {@link #releaseLocks(Lock[])}.
     * 
     * @param tx
     *            current transaction
     * @param user1
     *            one user
     * @param user2
     *            a second user
     * @return Array with two locks, where the elements are sorted DESC by the
     *         user's identifier. The locks have to be released from the last to
     *         the first array position.
     */
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

    /**
     * Locks a collection of users.<br>
     * What actually is locked, is the user node.<br>
     * The locks are registered to the current transaction and can be released
     * via this manager using {@link #releaseLocks(Transaction)}.
     * 
     * @param tx
     *            current transaction
     * @param users
     *            users that should be locked
     */
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

    /**
     * Releases an array with two locks. Locks will be released from the last to
     * the first array position.
     * 
     * @param locks
     *            array with locks of length 2
     */
    public static void releaseLocks(Lock[] locks) {
        locks[1].release();
        locks[0].release();
    }

    /**
     * Releases all locks registered to a transaction.
     * 
     * @param tx
     *            transaction with open locks
     */
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
