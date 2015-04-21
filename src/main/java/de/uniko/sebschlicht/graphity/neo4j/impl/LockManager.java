package de.uniko.sebschlicht.graphity.neo4j.impl;

import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

import de.uniko.sebschlicht.graphity.neo4j.model.UserProxy;

public class LockManager {

    protected static ConcurrentMap<Long, UserLock> LOCKS;

    protected static Queue<UserLock> UNUSED_LOCKS;

    static {
        LOCKS = new ConcurrentHashMap<>();
        UNUSED_LOCKS = new ConcurrentLinkedQueue<>();
    }

    protected static void lock(UserLock lock) {
        UserLock prevLock;
        StringBuilder message = new StringBuilder();
        message.append(lock);
        message.append(": locking [");
        for (UserProxy user : lock.getUsers()) {
            message.append(user.getIdentifier());
            message.append(",");
        }
        message.append("]");
        System.out.println(message.toString());
        try {
            for (UserProxy user : lock.getUsers()) {
                do {
                    prevLock = LOCKS.putIfAbsent(user.getIdentifier(), lock);
                    if (prevLock != null) {
                        System.out.println(lock + ": waiting for " + prevLock
                                + " to lock " + user.getIdentifier() + "...");
                        // wait until previous lock released
                        prevLock.await(100);
                        System.out.println(lock + ": retry to lock "
                                + user.getIdentifier() + "...");
                    }
                } while (prevLock != null);
                // user successfully locked
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(lock + ": locked.");
    }

    protected static UserLock getLock() {
        UserLock lock = UNUSED_LOCKS.poll();
        if (lock != null) {
            return lock;
        }
        return new UserLock();
    }

    /**
     * Locks a user.<br>
     * What actually is locked, is the user node.
     * The lock can be released via {@link #releaseLock(UserLock)}.
     * 
     * @param user
     *            user that has to be locked
     * @return lock acquired
     */
    public static UserLock lock(UserProxy user) {
        // create lock container
        UserLock lock = getLock();
        lock.add(user);
        // lock the user
        lock(lock);
        return lock;
    }

    /**
     * Locks two users.<br>
     * What actually is locked, is the user node.<br>
     * The lock can be released via {@link #releaseLock(UserLock)}.
     * 
     * @param user1
     *            one user
     * @param user2
     *            a second user
     * @return lock acquired for both users
     */
    public static UserLock lock(UserProxy user1, UserProxy user2) {
        // create lock container
        UserLock lock = getLock();
        lock.add(user1);
        lock.add(user2);
        // lock the users
        lock(lock);
        return lock;
    }

    /**
     * Locks a collection of users.<br>
     * What actually is locked, is the user node.<br>
     * The lock can be released via {@link #releaseLock(UserLock)}.
     * 
     * @param users
     *            users that should be locked
     * @return lock acquired for all the users
     */
    public static UserLock lock(Collection<UserProxy> users) {
        // create lock container
        UserLock lock = getLock();
        for (UserProxy user : users) {
            lock.add(user);
        }
        // lock the users
        lock(lock);
        return lock;
    }

    /**
     * Releases a lock.<br>
     * Unregisters locks of all users involved and notifies awaiting threads.
     * 
     * @param lock
     *            lock to release
     */
    public static void releaseLock(UserLock lock) {
        StringBuilder message = new StringBuilder();
        message.append(lock);
        message.append(": [");
        for (UserProxy user : lock.getUsers()) {
            LOCKS.remove(user.getIdentifier());
            message.append(user.getIdentifier());
            message.append(",");
        }
        message.append("] released.");
        lock.release();
        System.out.println(message.toString());
        UNUSED_LOCKS.add(lock);
    }
}
