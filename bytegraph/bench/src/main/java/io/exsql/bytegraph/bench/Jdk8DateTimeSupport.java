package io.exsql.bytegraph.bench;

import com.jsoniter.any.Any;
import com.jsoniter.output.JsonStream;
import com.jsoniter.spi.Encoder;
import com.jsoniter.spi.JsoniterSpi;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class Jdk8DateTimeSupport {

    private final static ThreadLocal<DateTimeFormatter> sdf = ThreadLocal.withInitial(
            () -> DateTimeFormatter.ISO_INSTANT
    );

    public static synchronized void enable() {
        JsoniterSpi.registerTypeEncoder(ZonedDateTime.class, new Encoder.ReflectionEncoder() {
            @Override
            public void encode(Object obj, JsonStream stream) throws IOException {
                stream.writeVal(((ZonedDateTime) obj).format(sdf.get()));
            }

            @Override
            public Any wrap(Object obj) {
                return Any.wrap(((ZonedDateTime) obj).format(sdf.get()));
            }
        });

        JsoniterSpi.registerTypeDecoder(
                ZonedDateTime.class,
                iter -> ZonedDateTime.parse(iter.readString(), sdf.get())
        );
    }

}
