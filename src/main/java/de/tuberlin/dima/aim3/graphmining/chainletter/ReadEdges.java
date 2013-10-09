/**
 * Graph-Mining Tutorial for Ozone
 *
 * Copyright (C) 2013  Sebastian Schelter <ssc@apache.org>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package de.tuberlin.dima.aim3.graphmining.chainletter;

import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

import java.util.regex.Pattern;

public class ReadEdges extends MapStub {

  private final PactRecord outputRecord = new PactRecord();
  private final PactInteger sourceVertex = new PactInteger();
  private final PactInteger targetVertex = new PactInteger();

  private static final Pattern SEPARATOR = Pattern.compile("[ \t,]");

  @Override
  public void map(PactRecord record, Collector<PactRecord> collector) throws Exception {

    String line = record.getField(0, PactString.class).getValue();

    if (line.startsWith("%")) {
      return;
    }

    String[] tokens = SEPARATOR.split(line);
    int source = Integer.parseInt(tokens[0]);
    int target = Integer.parseInt(tokens[1]);

    sourceVertex.setValue(source);
    targetVertex.setValue(target);

    outputRecord.setField(0, sourceVertex);
    outputRecord.setField(1, targetVertex);

    collector.collect(outputRecord);
  }
}
