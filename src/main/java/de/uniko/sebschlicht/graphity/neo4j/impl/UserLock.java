package de.uniko.sebschlicht.graphity.neo4j.impl;

import java.util.Set;
import java.util.TreeSet;

import de.uniko.sebschlicht.graphity.neo4j.model.UserProxy;

public class UserLock {

    protected Object _lock = new Object();

    protected Set<UserProxy> _users;

    public UserLock() {
        _users = new TreeSet<>(new LockUserComparator());
    }

    public Set<UserProxy> getUsers() {
        return _users;
    }

    public void add(UserProxy user) {
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
