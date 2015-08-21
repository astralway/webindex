
Observer Design Doc
===================

Sample Data

    a.com/page1 links to c.com, b.com
    b.com links to c.com/page1, c.com
    d.com links to c.com
    
  
Resulting Fluo Table

    row               cf        cq              value
    --------------------------------------------------
    d:com.a           domain    pagecount       1
    d:com.b           domain    pagecount       1
    d:com.c           domain    pagecount       2
    d:com.d           domain    pagecount       1
    p:com.a/page1     page      cur             {"outlinkcount": 2, "outlinks":[c.com, b.com]}
                      page      score           1
    p:com.b           inlinks   com.a/pag1      anchorText
                      page      cur             {"outlinkcount": 2, "outlinks":[c.com/page1, c.com]}
                      page      incount         1
                      page      score           2
    p:com.c           inlinks   com.a/page1     anchorText
                                com.b           anchorText
                                com.d           anchorText
                      page      incount         3
                                score           3
    p:com.c/page1     inlinks   com.b           anchorText
                      page      incount         1
                                score           1
    p:com.d           page      cur             {"outlinkcount": 1, "outlinks":[c.com]}
                      page      score           1


Resulting Accumulo Table

    row               cf        cq              value
    --------------------------------------------------
    d:com.a           domain    pagecount       1
                      rank      1:com.a/page1   1
    d:com.b           domain    pagecount       1
                      rank      2:com.b         2
    d:com.c           domain    pagecount       2
                      rank      3:com.c         3
                                1:com.c/page1   1
    d:com.d           domain    pagecount       1
                      rank      1:com.d         1
    p:com.a/page1     page      cur             {"outlinkcount": 2, "outlinks":[c.com, b.com]}
                      page      score           1
    p:com.b           inlinks   com.a/pag1      anchorText
                      page      cur             {"outlinkcount": 2, "outlinks":[c.com/page1, c.com]}
                      page      incount         1
                      page      score           2
    p:com.c           inlinks   com.a/page1     anchorText
                                com.b           anchorText
                                com.d           anchorText
                      page      incount         3
                                score           3
    p:com.c/page1     inlinks   com.b           anchorText
                      page      incount         1
                                score           1
    p:com.d           page      cur             {"outlinkcount": 1, "outlinks":[c.com]}
                      page      score           1
    t:incount         rank      3:com.c         3
                                2:com.b         1
                                1:com.c/page1   1
    t:pagecount       rank      2:com.c         2
                                1:com.a         1
                                1.com.b         1
    t:score           rank      3:com.c         3
                                2:com.b         2
                                1:com.c/page1   1
                                1:com.a/page1   1
                                1:com.d

Below are available operations:

    get(row, col) -> value
    set(row, col, value)
    del(row, col)

PageLoader is called with `pageUri` & `pageJson`

    curJson = get(pageUri, page:cur)
    if curJson != pageJson
      set(pageUri, page:new, pageJson)

PageObserver watches `page:new` is called with `pageUri`

    curJson = get(pageUri, page:cur)
    newJson = get(pageUri, page:new)

    newLinks,delLinks = compare(curJson, newJson)

    for link in newLinks:
      set(linkUri, inlinks-update:pageUri, [add,anchorText])

    for link in delLinks
      set(linkUri, inlinks-update:pageUri, [del])
      
    if newLinks.isNotEmpty() or delLinks.isNotEmpty():
      set(linkUri, inlinks-changed:ntfy, "")

    if curJson == null:
      pageScore = get(pageUri, stats:pagescore).toInteger(0)

      if pageScore != 0:
        del(pageDomain, rank:pageScore:pageUri)
      
      set(pageUri, stats:pagescore, pageScore+1)
      set(pageDomain, rank:pageScore+1:pageUri)

    set(pageUri, page:cur, newJson)
    del(pageUri, page:new)

InlinksObserver weakly watches `update:inlinks` is called with `pageUri`

    List<Update> updates = get(pageUri, update:inlinks)

    change = 0

    for update in updates:
      if update.action == del:
        change--
        del(pageUri, inlinks:update.linkUri)
      elif update.action == add:
        change++
        set(pageUri, inlinks:update.linkUri, update.anchorText)

    if change != 0:
      curCount = get(pageUri, stats:inlinkcount)
      curScore = get(pageUri, stats:pagescore).toInteger(0)

      if pageScore != 0:
        del(pageDomain, rank:curScore:pageUri)

      set(pageDomain, rank:curScore+change:pageUri)
      set(pageUri, stats:inlinkcount, curCount+change)
      set(pageUri, stats:pagescore, curScore+change)
