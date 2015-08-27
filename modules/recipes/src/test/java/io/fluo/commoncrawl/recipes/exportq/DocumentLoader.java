package io.fluo.commoncrawl.recipes.exportq;

import org.apache.commons.lang.StringUtils;

import io.fluo.api.types.TypedLoader;
import io.fluo.api.types.TypedTransactionBase;

public class DocumentLoader extends TypedLoader {

  String docid;
  String refs[];

  DocumentLoader(String docid, String... refs) {
    this.docid = docid;
    this.refs = refs;
  }

  @Override
  public void load(TypedTransactionBase tx, Context context) throws Exception {
    tx.mutate().row("d:" + docid).fam("content").qual("new").set(StringUtils.join(refs, " "));
  }
}
