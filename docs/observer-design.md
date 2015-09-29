
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
                                incount         0
    p:com.b           inlinks   com.a/pag1      anchorText
                      page      cur             {"outlinkcount": 2, "outlinks":[c.com/page1, c.com]}
                                incount         1
    p:com.c           inlinks   com.a/page1     anchorText
                                com.b           anchorText
                                com.d           anchorText
                      page      incount         3
    p:com.c/page1     inlinks   com.b           anchorText
                      page      incount         1
    p:com.d           page      cur             {"outlinkcount": 1, "outlinks":[c.com]}
                                incount         0
    

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
                                incount         0
    p:com.b           inlinks   com.a/pag1      anchorText
                      page      cur             {"outlinkcount": 2, "outlinks":[c.com/page1, c.com]}
                                incount         1
    p:com.c           inlinks   com.a/page1     anchorText
                                com.b           anchorText
                                com.d           anchorText
                      page      incount         3
    p:com.c/page1     inlinks   com.b           anchorText
                      page      incount         1
    p:com.d           page      cur             {"outlinkcount": 1, "outlinks":[c.com]}
                                incount         0
    t:incount         rank      3:com.c         3
                                2:com.b         2
                                1:com.c/page1   1
                                0:com.a/page1   0
                                0:com.d         0
