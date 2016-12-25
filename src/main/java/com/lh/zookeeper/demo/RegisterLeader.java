package com.lh.zookeeper.demo;

import org.apache.commons.collections.CollectionUtils;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Created by lh on 2016/12/23.
 */
public class RegisterLeader {
    private static CountDownLatch countDownLatch = new CountDownLatch(1);//同步计数器
    public static void main(String[] args) {
        int tryCount = 0;
        while (true) {
            try {
                ZooKeeper zk = new ZooKeeper("192.168.118.144:2181", 6000, new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                        System.out.println("current zk state : " + event.getState());
                        try {
                            // 连接建立时, 打开latch, 唤醒wait在该latch上的线程
                            if (event.getState() == Event.KeeperState.SyncConnected) {
                                countDownLatch.countDown();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
                // 等待连接建立
                countDownLatch.await();
                if (zk.exists("/chroot", true) != null) {
                    System.out.println("chroot is exist!");
                } else {
                    System.out.println("chroot is not exist!");
                    zk.create("/chroot", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
                String createStr = zk.create("/chroot/leader", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
                String leaderStr = createStr.substring(createStr.indexOf("/chroot/") + "/chroot/".length());
                List<String> lists = zk.getChildren("/chroot", false);
                boolean flag = false;
                if (CollectionUtils.isNotEmpty(lists)) {
                    Collections.sort(lists);
                    if (leaderStr.equals(lists.get(0))) {
                        flag = true;
                        System.out.println("Current point is leader");
                    } else {
                        System.out.println("Current point is not leader. Leader is " + lists.get(0));
                    }
                    if (!flag) {
                        if (lists.indexOf(leaderStr) - 1 >= 0) {
                            String previous = lists.get(lists.indexOf(leaderStr) - 1);
                            Stat stat = zk.exists("/chroot/" + previous, new Watcher() {
                                @Override
                                public void process(WatchedEvent event) {
                                    System.out.println(event.getPath() + "|" + event.getType().name());
                                    try {
                                        zk.exists(previous, this);
                                    } catch (KeeperException | InterruptedException e) {
                                        e.printStackTrace();
                                    }
                                }
                            });
                        }
                    }
                }
                Thread.sleep(1000000);
                zk.close();
            } catch (Exception e) {
                e.printStackTrace();
                tryCount++;
                if (tryCount > 3) {
                    try {
                        System.err.println("Retry connection to Zookeeper failure!");
                        break;
                    } catch (Exception e1) {
                        e1.printStackTrace();
                    }
                } else {
                    System.err.println("Retry connection to Zookeeper success! count: " + tryCount);
                }
            }
        }
    }
}
