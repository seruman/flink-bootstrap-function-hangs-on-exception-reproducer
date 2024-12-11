package me.seruman;

import java.util.Map;
import java.util.Map.Entry;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.state.api.OperatorTransformation;
import org.apache.flink.state.api.SavepointWriter;
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Main {

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final String destination = params.getRequired("destination");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        setupJob(env, destination);
        env.execute("bootstrap-state");
    }

    public static void setupJob(StreamExecutionEnvironment env, String destination) {
        var stream = env.fromSequence(1, 1000);

        KeySelector<Long, Long> keySelector = (v) -> v % 2;
        SavepointWriter.newSavepoint(env, env.getMaxParallelism())
                .withOperator(
                        OperatorIdentifier.forUid(Op1BootstrapFunction.UID),
                        OperatorTransformation.bootstrapWith(stream)
                                .keyBy(keySelector)
                                .transform(new Op1BootstrapFunction()))
                .withOperator(
                        OperatorIdentifier.forUid(Op2BootstrapFunction.UID),
                        OperatorTransformation.bootstrapWith(stream)
                                .keyBy(keySelector)
                                .transform(new Op2BootstrapFunction()))
                .write(destination);
    }

    static class Op1BootstrapFunction extends KeyedStateBootstrapFunction<Long, Long> {
        static final String UID = "OP1";

        static final ListStateDescriptor<Long> STATE_DESCRIPTOR = new ListStateDescriptor<>("state", Long.class);

        private ListState<Long> state;

        @Override
        public void open(OpenContext openContext) throws Exception {
            state = getRuntimeContext().getListState(STATE_DESCRIPTOR);
        }

        @Override
        public void processElement(Long value, KeyedStateBootstrapFunction<Long, Long>.Context ctx) throws Exception {
            state.add(value);
        }

        static class StateReader extends KeyedStateReaderFunction<Long, Entry<Long, Long>> {
            private ListState<Long> state;

            @Override
            public void open(Configuration parameters) throws Exception {
                state = getRuntimeContext().getListState(STATE_DESCRIPTOR);
            }

            @Override
            public void readKey(Long key, Context ctx, Collector<Entry<Long, Long>> out) throws Exception {
                for (Long v : state.get()) {
                    out.collect(Map.entry(key, v));
                }
            }
        }
    }

    static class Op2BootstrapFunction extends KeyedStateBootstrapFunction<Long, Long> {
        static final String UID = "OP2";

        static final ListStateDescriptor<Long> STATE_DESCRIPTOR = new ListStateDescriptor<>("state", Long.class);

        private ListState<Long> state;

        @Override
        public void open(OpenContext openContext) throws Exception {
            state = getRuntimeContext().getListState(STATE_DESCRIPTOR);
        }

        @Override
        public void processElement(Long value, KeyedStateBootstrapFunction<Long, Long>.Context ctx) throws Exception {
            if (value > 10) {
                throw new RuntimeException("boom");
            }

            state.add(value * 10);
        }

        static class StateReader extends KeyedStateReaderFunction<Long, Entry<Long, Long>> {
            private ListState<Long> state;

            @Override
            public void open(Configuration parameters) throws Exception {
                state = getRuntimeContext().getListState(STATE_DESCRIPTOR);
            }

            @Override
            public void readKey(Long key, Context ctx, Collector<Entry<Long, Long>> out) throws Exception {
                for (Long v : state.get()) {
                    out.collect(Map.entry(key, v));
                }
            }
        }
    }
}

