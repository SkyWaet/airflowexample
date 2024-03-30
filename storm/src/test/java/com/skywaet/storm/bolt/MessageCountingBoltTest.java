package com.skywaet.storm.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.Mockito.*;

class MessageCountingBoltTest {

    @Test
    public void shouldProcessDate() {
        var bolt = new MessageCountingBolt();
        var outputCollector = mock(OutputCollector.class);
        bolt.prepare(Map.of(), mock(TopologyContext.class), outputCollector);

        var window = mock(TupleWindow.class);
        var tuple1 = mock(Tuple.class);
        when(tuple1.getStringByField("timestamp")).thenReturn("2024-03-30T21:49:05.378Z");
        when(tuple1.getStringByField("level")).thenReturn("ERROR");

        var tuple2 = mock(Tuple.class);
        when(tuple2.getStringByField("timestamp")).thenReturn("2024-03-30T21:49:38.378Z");
        when(tuple2.getStringByField("level")).thenReturn("WARN");

        var tuple3 = mock(Tuple.class);
        when(tuple3.getStringByField("timestamp")).thenReturn("2024-03-30T21:50:05.378Z");
        when(tuple3.getStringByField("level")).thenReturn("ERROR");


        when(window.get()).thenReturn(List.of(tuple1, tuple2, tuple3));

        var argumentCaptor = ArgumentCaptor.forClass(Values.class);

        assertThatNoException()
                .isThrownBy(() -> bolt.execute(window));

        verify(outputCollector, times(2)).emit(argumentCaptor.capture());

        assertThat(argumentCaptor.getAllValues().stream().flatMap(Collection::stream).collect(Collectors.toList()))
                .asInstanceOf(InstanceOfAssertFactories.list(Object.class))
                .containsExactlyInAnyOrder("{\"timestamp\":1711835400,\"warnLogs\":0,\"errorLogs\":1}",
                        "{\"timestamp\":1711835340,\"warnLogs\":1,\"errorLogs\":1}");
    }

}