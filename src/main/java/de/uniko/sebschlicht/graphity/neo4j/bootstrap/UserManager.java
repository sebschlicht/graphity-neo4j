package de.uniko.sebschlicht.graphity.neo4j.bootstrap;

import java.util.HashMap;
import java.util.Map;

public class UserManager {

    private long[] _userIds;

    private Map<Long, User> _users;

    public UserManager() {
        _users = new HashMap<Long, User>();
    }

    public void setUserIds(long[] userIds) {
        _userIds = userIds;
    }

    public User getUserByIndex(int iUser) {
        long id = _userIds[iUser];
        return getUser(id);
    }

    public User loadUserByIndex(int iUser) {
        long id = _userIds[iUser];
        return loadUser(id);
    }

    public User addUserByIndex(int iUser) {
        long id = _userIds[iUser];
        return addUser(id);
    }

    public User getUser(long id) {
        return _users.get(id);
    }

    public User loadUser(long id) {
        User user = _users.get(id);
        if (user != null) {
            return user;
        }
        return addUser(id);
    }

    public User addUser(long id) {
        User user = new User();
        _users.put(id, user);
        return user;
    }
}
