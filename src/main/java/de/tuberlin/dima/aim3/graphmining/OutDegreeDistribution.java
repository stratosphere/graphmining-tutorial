package de.tuberlin.dima.aim3.graphmining;

import eu.stratosphere.pact.client.LocalExecutor;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

import java.io.Serializable;
import java.util.Iterator;
import java.util.regex.Pattern;

public class OutDegreeDistribution implements PlanAssembler {

  @Override
  public Plan getPlan(String... args) {

    if (args.length != 3) {
      System.err.println("numSubtasks input-path output-path");
      System.exit(-1);
    }

    int numSubTasks = Integer.parseInt(args[0]);
    String inputPath = args[1];
    String outputPath = args[2];

    FileDataSource source = new FileDataSource(new TextInputFormat(), inputPath, "ReadEdges");

    MapContract edgeMap = MapContract.builder(EdgeMap.class)
        .input(source)
        .name("ParseEdges")
        .build();

    ReduceContract degreePerVertex = ReduceContract.builder(DegreePerVertex.class)
        .input(edgeMap)
        .keyField(PactInteger.class, 0)
        .name("DegreePerVertex")
        .build();

    ReduceContract sumDegrees = ReduceContract.builder(SumDegrees.class)
        .input(degreePerVertex)
        .keyField(PactInteger.class, 0)
        .name("SumDegrees")
        .build();

    FileDataSink out = new FileDataSink(new RecordOutputFormat(), outputPath, sumDegrees, "Degrees");
    RecordOutputFormat.configureRecordFormat(out)
        .recordDelimiter('\n')
        .fieldDelimiter(' ')
        .field(PactInteger.class, 0)
        .field(PactInteger.class, 1);

    Plan plan = new Plan(out, "OutDegreeDistribution");
    plan.setDefaultParallelism(numSubTasks);
    return plan;
  }

  public static class EdgeMap extends MapStub implements Serializable {

    private final PactRecord outputRecord = new PactRecord();
    private final PactInteger outputVertex = new PactInteger();
    private final PactInteger one = new PactInteger(1);

    private static final Pattern SEPARATOR = Pattern.compile("[ \t,]");

    @Override
    public void map(PactRecord record, Collector<PactRecord> collector) throws Exception {

      String line = record.getField(0, PactString.class).getValue();

      String[] tokens = SEPARATOR.split(line);
      int sourceVertex = Integer.parseInt(tokens[0]);

      outputVertex.setValue(sourceVertex);
      outputRecord.setField(0, outputVertex);
      outputRecord.setField(1, one);

      collector.collect(outputRecord);
    }
  }

  public static class DegreePerVertex extends ReduceStub implements Serializable {

    private final PactRecord outputRecord = new PactRecord();
    private final PactInteger pactDegree = new PactInteger();
    private final PactInteger one = new PactInteger();

    @Override
    public void reduce(Iterator<PactRecord> records, Collector<PactRecord> collector) throws Exception {

      int degree = 0;
      while (records.hasNext()) {
        records.next();
        degree++;
      }

      pactDegree.setValue(degree);
      outputRecord.setField(0, pactDegree);
      outputRecord.setField(1, one);
      collector.collect(outputRecord);
    }
  }

  @ReduceContract.Combinable
  public static class SumDegrees extends ReduceStub implements Serializable {

    private final PactInteger pactCount = new PactInteger();

    @Override
    public void reduce(Iterator<PactRecord> records, Collector<PactRecord> collector) throws Exception {

      PactRecord firstRecord = records.next();
      int count = 1;
      while (records.hasNext()) {
        records.next();
        count++;
      }

      pactCount.setValue(count);
      firstRecord.setField(1, pactCount);
      collector.collect(firstRecord);
    }
  }

  public static void main(String[] args) throws Exception {

    Plan plan = new OutDegreeDistribution().getPlan(String.valueOf(1),
        "file:////home/ssc/Desktop/tmp/slashdot-zoo/slashdot-zoo.csv",
        "file:///tmp/ozone/outdegrees");
    LocalExecutor.execute(plan);
    System.exit(0);
  }
}
