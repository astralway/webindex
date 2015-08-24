package io.fluo.commoncrawl.recipes.exportq;

import java.util.HashSet;
import java.util.Set;

import io.fluo.commoncrawl.recipes.serialization.SimpleSerializer;

class RefUpdates {
  private Set<String> addedRefs;
  private  Set<String> deletedRefs;
  
  RefUpdates(Set<String> addedRefs, Set<String> deletedRefs){
    this.addedRefs = addedRefs;
    this.deletedRefs = deletedRefs;
  }
  
  Set<String> getAddedRefs(){
    return addedRefs;
  }
  
  Set<String> getDeletedRefs(){
    return deletedRefs;
  }
  
  @Override
  public String toString(){
    return "added:"+addedRefs+" deleted:"+deletedRefs;
  }
  
  static SimpleSerializer<RefUpdates> newSerializer(){
    return new SimpleSerializer<RefUpdates>(){
      @Override
      public RefUpdates deserialize(byte[] b) {
        String[] rus = new String(b).split(",");
        
        Set<String> addedRefs = new HashSet<>();
        Set<String> deletedRefs = new HashSet<>();
        
        for (String update : rus) {      
          if(update.startsWith("a")) {
            addedRefs.add(update.substring(1));
          } else if(update.startsWith("d")) {
            deletedRefs.add(update.substring(1));
          }
        }
        return new RefUpdates(addedRefs, deletedRefs);
      }

      @Override
      public byte[] serialize(RefUpdates e) {
        StringBuilder sb = new StringBuilder();
        String sep = "";
        for(String ar : e.getAddedRefs()) {
          sb.append(sep);
          sep = ",";
          sb.append("a");
          sb.append(ar);
        }
        
        for(String dr : e.getDeletedRefs()) {
          sb.append(sep);
          sep = ",";
          sb.append("d");
          sb.append(dr);
        }
        
        return sb.toString().getBytes();
      }};
  }
}