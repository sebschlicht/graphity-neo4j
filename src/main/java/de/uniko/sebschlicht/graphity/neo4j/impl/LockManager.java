package de.uniko.sebschlicht.graphity.neo4j.impl;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;

import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.DeadlockDetectedException;

import de.uniko.sebschlicht.graphity.neo4j.model.UserProxy;

public class LockManager {

    /**
     * Locks a user.<br>
     * What actually is locked, is the user node.
     * 
     * @param tx
     *            current transaction
     * @param user
     *            user that has to be locked
     * @return lock acquired from the transaction
     */
    public static Lock lock(Transaction tx, UserProxy user) {
        return tx.acquireWriteLock(user.getNode());
    }

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
            System.out.println(tx + ": locking manager locked ["
                    + user1.getNode() + " (" + user1.getIdentifier() + "), "
                    + user2.getNode() + " (" + user2.getIdentifier() + ")]");
        } else {
            locks[0] = tx.acquireWriteLock(user2.getNode());
            locks[1] = tx.acquireWriteLock(user1.getNode());
            System.out.println(tx + ": locking manager locked ["
                    + user2.getNode() + " (" + user2.getIdentifier() + "), "
                    + user1.getNode() + " (" + user1.getIdentifier() + ")]");
        }
        return locks;
    }

    /**
     * Locks a collection of users.<br>
     * What actually is locked, is the user node.<br>
     * The locks can be released using {@link #releaseLocks(List)}.
     * 
     * @param tx
     *            current transaction
     * @param users
     *            users that should be locked
     * @return list with all user locks, has to be released from first to last
     */
    public static List<Lock> lock(Transaction tx, Collection<UserProxy> users) {
        TreeSet<UserProxy> userSet = new TreeSet<>(new LockUserComparator());
        for (UserProxy user : users) {
            userSet.add(user);
        }

        StringBuilder debug = new StringBuilder();
        debug.append(tx);
        debug.append(": locking manager locked [");
        LinkedList<Lock> locks = new LinkedList<>();
        for (UserProxy user : userSet) {
            debug.append(user.getNode());
            debug.append(" (");
            debug.append(user.getIdentifier());
            debug.append(")");
            Lock lock;
            try {
                lock = tx.acquireWriteLock(user.getNode());
            } catch (DeadlockDetectedException e) {
                System.err.println(debug.toString());
                throw e;
            }
            debug.append(",");
            locks.addFirst(lock);
        }
        debug.append("]");
        return locks;
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
     * Releases all locks in a list from first to last.
     * 
     * @param locks
     *            list of user locks
     */
    public static void releaseLocks(List<Lock> locks) {
        if (locks == null) {
            return;
        }
        for (Lock lock : locks) {
            lock.release();
        }
        locks.clear();
    }
}
