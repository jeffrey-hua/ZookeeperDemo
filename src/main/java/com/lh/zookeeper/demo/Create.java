package com.lh.zookeeper.demo;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;

/**
 * Created by liuhua on 2016/12/25.
 */
public class Create {
    public static void main(String[] args){
        try {
            ZooKeeper zk = new ZooKeeper("192.168.118.144:2181",6000,new Watcher(){
                @Override
                public void process(WatchedEvent event){
                    System.out.println("current zk state : " + event.getState());
                }
            });

//            final AsyncCallback.ChildrenCallback callback = new AsyncCallback.ChildrenCallback() {
//                @Override
//                public void processResult(int rc, String path, Object ctx, List<String> children) {
//                    System.out.println( "current :" + children);
//                }
//            };

            Watcher watcher = new Watcher() {
                @Override
                public void process(WatchedEvent event) {
//                    System.out.println("Event is " + event);
//                    if (event.getType() == Event.EventType.NodeChildrenChanged) {
//                        System.out.println("Changed " + event);
//                        zk.getChildren("/chroot", this, callback, null);
//                    }
                }
            };

//            zk.create("/chroot", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/chroot/leader", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            Thread.sleep(1000);
            System.out.println(zk.getChildren("/chroot", watcher));
            zk.create("/chroot/leader", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            Thread.sleep(1000);
//            zk.getChildren("/chroot", watcher, callback, null);
            System.out.println(zk.getChildren("/chroot", watcher));
            zk.create("/chroot/leader", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            Thread.sleep(1000);
//            zk.getChildren("/chroot", watcher, callback, null);
            System.out.println(zk.getChildren("/chroot", watcher));
            List<String> lists = zk.getChildren("/chroot", watcher);
            Collections.sort(lists);
            for(String s:lists){
                System.out.println(s);
            }
            Stat stat = zk.exists("/chroot", new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    System.out.println(event.getPath() + "|" + event.getType().name());
                    try{
                        zk.exists("/chroot",this);
                    } catch (KeeperException | InterruptedException e){
                        e.printStackTrace();
                    }
                }
            });
//            Thread.sleep(10000);
//            zk.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
