package de.uniko.sebschlicht.graphity.neo4j.impl;

import java.util.Set;
import java.util.TreeSet;

public class UserLock {

    protected Object _lock = new Object();

    protected Set<LockableUser> _users;

    public UserLock() {
        _users = new TreeSet<>(new LockUserComparator());
    }

    public Set<LockableUser> getUsers() {
        return _users;
    }

    public void add(LockableUser user) {
        _users.add(user);
    }

    public void release() {
        _users.clear();
        synchronized (_lock) {
            _lock.notifyAll();
        }
    }

    public void await(long timeout) throws InterruptedException {
        synchronized (_lock) {
            _lock.wait(timeout);
        }
    }
}
