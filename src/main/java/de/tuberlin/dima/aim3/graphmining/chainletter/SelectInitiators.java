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

import de.tuberlin.dima.aim3.graphmining.Config;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactBoolean;
import eu.stratosphere.pact.common.type.base.PactInteger;

import java.util.Random;

public class SelectInitiators extends MapStub {

  private final Random random = new Random(Config.randomSeed());

  private final PactInteger vertexID = new PactInteger();
  private final PactBoolean seedVertex = new PactBoolean();
  private final PactRecord outputRecord = new PactRecord();

  @Override
  public void map(PactRecord record, Collector<PactRecord> collector) throws Exception {

    boolean isSeedVertex = random.nextDouble() < ChainLetter.INITIATOR_RATIO;

    vertexID.setValue(record.getField(0, PactInteger.class).getValue());
    seedVertex.setValue(isSeedVertex);

    outputRecord.setField(0, vertexID);
    outputRecord.setField(1, seedVertex);

    collector.collect(outputRecord);
  }
}
