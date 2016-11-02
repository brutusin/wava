/*
 * Copyright 2016 Ignacio del Valle Alles idelvall@brutusin.org.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.brutusin.wava.core;

import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeSet;

/**
 *
 * @author Ignacio del Valle Alles idelvall@brutusin.org
 */
public class JobSet {

    public enum State {

        queued, running
    };

    private final HashMap<Integer, Key> keyMap = new HashMap<>();
    private final TreeSet<Key> queueTree = new TreeSet<>();
    private final TreeSet<Key> runningTree = new TreeSet<>();

    /**
     * synchronization on JobSet instance needed for iteration
     *
     * @return
     */
    public QueueIterator getQueue() {
        final Iterator<Key> it = queueTree.iterator();
        return new QueueIterator() {
            private Key last;

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public Integer next() {
                this.last = it.next();
                return last.getId();
            }

            @Override
            public void moveToRunning() {
                it.remove();
                runningTree.add(last);
            }

            @Override
            public void remove() {
                it.remove();
                keyMap.remove(last.getId());
            }
        };
    }

    /**
     * synchronization on JobSet instance needed for iteration
     *
     * @return
     */
    public RunningIterator getRunning() {
        final Iterator<Key> it = runningTree.iterator();
        return new RunningIterator() {
            private Key last;

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public Integer next() {
                this.last = it.next();
                return last.getId();
            }

            @Override
            public void remove() {
                it.remove();
                keyMap.remove(last.getId());
            }
        };
    }

    private Key getKey(int id) {
        Key key = keyMap.get(id);
        if (key == null) {
            throw new IllegalArgumentException("Id " + id + " is not in job set");
        }
        return key;
    }

    public synchronized void queue(int id, int majorPriority, int minorPriority) {
        if (keyMap.containsKey(id)) {
            throw new IllegalArgumentException("Id " + id + " already is in job set");
        }
        Key key = new Key(id, majorPriority, minorPriority);
        keyMap.put(id, key);
        queueTree.add(key);
    }

    public synchronized void remove(int id) {
        Key key = this.keyMap.remove(id);
        if (key == null) {
            return;
        }
        if (!queueTree.remove(key)) {
            runningTree.remove(key);
        }
    }

    public synchronized void run(int id) {
        Key key = getKey(id);
        if (queueTree.remove(key)) {
            runningTree.add(key);
        } else {
            throw new IllegalArgumentException("Id " + id + " is not queued");
        }
    }

    public synchronized void setPriority(int id, int majorPriority, int minorPriority) {
        Key key = getKey(id);
        if (key.getMajorPriority() == majorPriority && key.getMinorPriority() == minorPriority) {
            return;
        }
        TreeSet<Key> tree;
        if (queueTree.remove(key)) {
            tree = queueTree;
        } else if (runningTree.remove(key)) {
            tree = runningTree;
        } else {
            throw new AssertionError();
        }
        key.setMajorPriority(majorPriority);
        key.setMinorPriority(minorPriority);
        tree.add(key);
    }

    public synchronized int countQueued() {
        return queueTree.size();
    }

    public synchronized int countRunning() {
        return runningTree.size();
    }

    public synchronized State getState(int id) {
        Key key = this.keyMap.get(id);
        if (key == null) {
            return null;
        }
        if (queueTree.contains(key)) {
            return State.queued;
        } else if (runningTree.contains(key)) {
            return State.running;
        } else {
            throw new AssertionError();
        }

    }

    public class Key implements Comparable<Key> {

        private final int id;
        private int majorPriority;
        private int minorPriority;

        public Key(int id, int majorPriority, int minorPriority) {
            this.majorPriority = majorPriority;
            this.minorPriority = minorPriority;
            this.id = id;
        }

        public int getMajorPriority() {
            return majorPriority;
        }

        public void setMajorPriority(int majorPriority) {
            this.majorPriority = majorPriority;
        }

        public int getMinorPriority() {
            return minorPriority;
        }

        public void setMinorPriority(int minorPriority) {
            this.minorPriority = minorPriority;
        }

        public int getId() {
            return id;
        }

        @Override
        public int compareTo(Key o) {
            int ret = Integer.compare(majorPriority, o.majorPriority);
            if (ret == 0) {
                ret = Integer.compare(minorPriority, o.minorPriority);
                if (ret == 0) {
                    ret = Integer.compare(id, o.id);
                }
            }
            return ret;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || !(obj instanceof Key)) {
                return false;
            }
            Key other = (Key) obj;
            return id == other.id;
        }

        @Override
        public int hashCode() {
            return id;
        }
    }

    public interface QueueIterator extends Iterator<Integer> {

        public void moveToRunning();
    }

    public interface RunningIterator extends Iterator<Integer> {
    }
}
