/*
 * Copyright 2015 Fluo authors (see AUTHORS)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.fluo.webindex.serialization;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Optional;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.pool.KryoFactory;
import io.fluo.webindex.core.models.Link;
import io.fluo.webindex.data.fluo.DomainExport;
import io.fluo.webindex.data.fluo.PageExport;
import io.fluo.webindex.data.fluo.UriCountExport;
import io.fluo.webindex.data.fluo.UriMap.UriInfo;

public class WebindexKryoFactory implements KryoFactory, Serializable {

  private static final long serialVersionUID = 1L;

  @Override
  public Kryo create() {
    Kryo kryo = new Kryo();

    // Explicitly set class ids when registering. Did not set ids (because thought if registered in
    // same order it would be ok) and ran into issue where Spark and Fluo code were using different
    // ids for some reason.
    kryo.register(UriInfo.class, 9);
    kryo.register(DomainExport.class, 10);
    kryo.register(UriCountExport.class, 11);
    kryo.register(PageExport.class, 12);
    kryo.register(ArrayList.class, 13);
    kryo.register(Link.class, 14);
    kryo.register(Optional.class, 15);

    kryo.setRegistrationRequired(true);

    return kryo;
  }

}
