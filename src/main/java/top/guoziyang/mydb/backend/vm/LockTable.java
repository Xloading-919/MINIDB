package top.guoziyang.mydb.backend.vm;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.common.Error;

/**
 * 精细化锁管理版本：为每个 UID 维护一把锁和一个条件变量。
 */
public class LockTable {

    private final Map<Long, Set<Long>> x2u = new HashMap<>();          // XID 持有的 UID 集合
    private final Map<Long, Long> u2x = new HashMap<>();               // UID 被哪个 XID 持有
    private final Map<Long, Long> waitU = new HashMap<>();            // XID 正在等待的 UID
    private final Map<Long, ResourceLock> resourceLocks = new ConcurrentHashMap<>(); // UID 对应的锁对象

    private static class ResourceLock {
        final ReentrantLock lock = new ReentrantLock();
        final Condition condition = lock.newCondition();
        final Queue<Long> waitingQueue = new LinkedList<>();
    }

    public void add(long xid, long uid) throws Exception {
        ResourceLock rLock = resourceLocks.computeIfAbsent(uid, k -> new ResourceLock());

        rLock.lock.lock();
        try {
            // 事务已持有该资源
            if (x2u.getOrDefault(xid, Set.of()).contains(uid)) return;

            // 资源空闲，直接占用
            if (!u2x.containsKey(uid)) {
                u2x.put(uid, xid);
                x2u.computeIfAbsent(xid, k -> new HashSet<>()).add(uid);
                return;
            }

            // 否则等待
            waitU.put(xid, uid);
            rLock.waitingQueue.add(xid);

            if (hasDeadLock()) {
                waitU.remove(xid);
                rLock.waitingQueue.remove(xid);
                throw Error.DeadlockException;
            }

            while (u2x.containsKey(uid) || rLock.waitingQueue.peek() != xid) {
                rLock.condition.await();
            }

            // 获得资源
            rLock.waitingQueue.poll();
            waitU.remove(xid);
            u2x.put(uid, xid);
            x2u.computeIfAbsent(xid, k -> new HashSet<>()).add(uid);

        } finally {
            rLock.lock.unlock();
        }
    }

    public void remove(long xid) {
        Set<Long> held = x2u.getOrDefault(xid, Set.of());
        for (long uid : new HashSet<>(held)) {
            release(uid, xid);
        }
        x2u.remove(xid);
        waitU.remove(xid);
    }

    private void release(long uid, long xid) {
        ResourceLock rLock = resourceLocks.get(uid);
        if (rLock == null) return;

        rLock.lock.lock();
        try {
            if (u2x.get(uid) != xid) return;
            u2x.remove(uid);
            x2u.getOrDefault(xid, Set.of()).remove(uid);
            rLock.condition.signalAll();
        } finally {
            rLock.lock.unlock();
        }
    }

    private boolean hasDeadLock() {
        Map<Long, Integer> xidStamp = new HashMap<>();
        int stamp = 1;
        for (long xid : x2u.keySet()) {
            if (xidStamp.getOrDefault(xid, 0) > 0) continue;
            stamp++;
            if (dfs(xid, xidStamp, stamp)) return true;
        }
        return false;
    }

    private boolean dfs(long xid, Map<Long, Integer> stampMap, int stamp) {
        Integer s = stampMap.get(xid);
        if (s != null && s == stamp) return true;
        if (s != null && s < stamp) return false;
        stampMap.put(xid, stamp);

        Long uid = waitU.get(xid);
        if (uid == null) return false;
        Long holder = u2x.get(uid);
        if (holder == null) return false;
        return dfs(holder, stampMap, stamp);
    }
}
