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
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactBoolean;

import java.util.Random;

public class InitialForwards extends MatchStub {

  private final Random random = new Random(Config.randomSeed());

  @Override
  public void match(PactRecord seedVertexCandidate, PactRecord edge, Collector<PactRecord> collector) throws Exception {

    boolean isSeedVertex = seedVertexCandidate.getField(1, PactBoolean.class).getValue();
    if (isSeedVertex && random.nextDouble() < ChainLetter.FORWARDING_PROBABILITY) {
      collector.collect(edge);
    }
  }
}
