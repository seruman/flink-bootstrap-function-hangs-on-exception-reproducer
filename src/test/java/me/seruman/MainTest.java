package me.seruman;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;
import org.apache.flink.api.common.BatchShuffleMode;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.JobManagerOptions.SchedulerType;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.state.api.SavepointReader;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

class MainTest {

    private static final int PARALLELISM = 2;
    private static final int MAX_PARALLELISM = 10;

    @RegisterExtension
    public static MiniClusterExtension flinkCluster =
            new MiniClusterExtension(new MiniClusterResourceConfiguration.Builder()
                    .setNumberSlotsPerTaskManager(1)
                    .setNumberTaskManagers(PARALLELISM)
                    .build());

    @Test
    void test(@TempDir Path tempDir) throws Exception {
        final String outputPath = tempDir.resolve("savepoint").toString();

        var config = new Configuration();
        config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        config.set(ExecutionOptions.BATCH_SHUFFLE_MODE, BatchShuffleMode.ALL_EXCHANGES_PIPELINED);
        config.set(JobManagerOptions.SCHEDULER, SchedulerType.Default);

        var env = TestStreamEnvironment.getExecutionEnvironment(config);
        env.setMaxParallelism(MAX_PARALLELISM);

        Main.setupJob(env, outputPath);

        var result = env.execute();

        System.out.println(result);

        dumpSavepoint(env, outputPath);
    }

    private void dumpSavepoint(StreamExecutionEnvironment env, String path) throws Exception {
        var reader = SavepointReader.read(env, path);

        var op1State = new HashMap<Long, List<Long>>();
        reader.readKeyedState(
                        OperatorIdentifier.forUid(Main.Op1BootstrapFunction.UID),
                        new Main.Op1BootstrapFunction.StateReader())
                .executeAndCollect()
                .forEachRemaining(entryCollector(op1State));

        var op2State = new HashMap<Long, List<Long>>();
        reader.readKeyedState(
                        OperatorIdentifier.forUid(Main.Op2BootstrapFunction.UID),
                        new Main.Op2BootstrapFunction.StateReader())
                .executeAndCollect()
                .forEachRemaining(entryCollector(op2State));

        System.out.println(Main.Op1BootstrapFunction.UID + " state:" + op1State);
        System.out.println(Main.Op2BootstrapFunction.UID + " state:" + op2State);
    }

    static Consumer<? super Entry<Long, Long>> entryCollector(Map<Long, List<Long>> to) {
        Consumer<? super Entry<Long, Long>> entryCollector = entry -> {
            var key = entry.getKey();
            var value = entry.getValue();
            to.computeIfAbsent(key, _k -> new ArrayList<>()).add(value);
        };

        return entryCollector;
    }
}
