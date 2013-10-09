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
import eu.stratosphere.pact.client.LocalExecutor;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.type.base.PactBoolean;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.generic.contract.WorksetIteration;

public class ChainLetter implements PlanAssembler {

  public static final double SEED_RATIO = 0.00125;

  public static final double FORWARDING_PROBABILITY = 0.5;

  @Override
  public Plan getPlan(String... args) {
    if (args.length != 3) {
      System.err.println("<numSubtasks> <inputPath> <outputPath>");
      System.exit(-1);
    }

    int numSubTasks = Integer.parseInt(args[0]);
    String inputPath = args[1];
    String outputPath = args[2];

    FileDataSource source = new FileDataSource(new TextInputFormat(), inputPath, "ReadEdges");

    MapContract edgesToVertices = MapContract.builder(EdgeToVertices.class)
        .input(source)
        .name("EdgesToVertices")
        .build();

    ReduceContract uniqueVertexIDs = ReduceContract.builder(UniqueVertexIDs.class)
        .input(edgesToVertices)
        .keyField(PactInteger.class, 0)
        .name("UniqueVertexIDs")
        .build();

    MapContract letterInitiators = MapContract.builder(SelectInitiators.class)
        .input(uniqueVertexIDs)
        .name("SelectInitiators")
        .build();

    MapContract readEdges = MapContract.builder(ReadEdges.class)
        .input(source)
        .name("ReadEdges")
        .build();

    MatchContract initialForwards = MatchContract.builder(InitialForwards.class, PactInteger.class, 0, 0)
        .input1(letterInitiators)
        .input2(readEdges)
        .name("InitialMessages")
        .build();

    WorksetIteration iteration = new WorksetIteration(0, "ChainLetter Simulation");
    iteration.setInitialSolutionSet(letterInitiators);
    iteration.setInitialWorkset(initialForwards);
    iteration.setMaximumNumberOfIterations(250);

    ReduceContract deliverMessage = ReduceContract.builder(DeliverMessage.class)
        .input(iteration.getWorkset())
        .keyField(PactInteger.class, 1)
        .name("DeliverMessage")
        .build();

    MatchContract receiveMessage = MatchContract.builder(ReceiveMessage.class, PactInteger.class, 0, 0)
        .input1(deliverMessage)
        .input2(iteration.getSolutionSet())
        .name("ReceiveMessage")
        .build();

    MatchContract forwardMessage = MatchContract.builder(ForwardToFriend.class, PactInteger.class, 0, 0)
        .input1(iteration.getSolutionSetDelta())
        .input2(readEdges)
        .name("ForwardToFriend")
        .build();

    iteration.setNextWorkset(forwardMessage);
    iteration.setSolutionSetDelta(receiveMessage);

    FileDataSink out = new FileDataSink(new RecordOutputFormat(), outputPath, iteration, "Output");
    RecordOutputFormat.configureRecordFormat(out)
        .recordDelimiter('\n')
        .fieldDelimiter(' ')
        .field(PactInteger.class, 0)
        .field(PactBoolean.class, 1);

    Plan plan = new Plan(out, "ChainLetter");
    plan.setDefaultParallelism(numSubTasks);
    return plan;
  }

  public static void main(String[] args) throws Exception {

    Plan plan = new ChainLetter().getPlan(String.valueOf(Config.numberOfSubtasks()),
        Config.pathToSlashdotZoo(), Config.outputPath() + "chainLetter");
    LocalExecutor.execute(plan);
    System.exit(0);
  }


}
