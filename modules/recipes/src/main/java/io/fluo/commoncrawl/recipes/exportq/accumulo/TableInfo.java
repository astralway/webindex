package io.fluo.commoncrawl.recipes.exportq.accumulo;

public class TableInfo {
  String instanceName;
  String zookeepers;
  String user;
  String password;
  String table;

  public TableInfo(String instanceName, String zookeepers, String user, String password,
      String table) {
    this.instanceName = instanceName;
    this.zookeepers = zookeepers;
    this.user = user;
    this.password = password;
    this.table = table;
  }
}
