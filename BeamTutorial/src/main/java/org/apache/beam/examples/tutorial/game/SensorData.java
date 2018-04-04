package org.apache.beam.examples.tutorial.game;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

import java.io.Serializable;

@DefaultCoder(SerializableCoder.class)
public class SensorData implements Serializable {
    private String deviceId;
    private Double temperature;

    public SensorData() {
    }

    public SensorData(String deviceId, Double temperature) {
        this.deviceId = deviceId;
        this.temperature = temperature;
    }

    public String getDeviceId() {
        return this.deviceId;
    }

    public Double getTemperature() {
        return this.temperature;
    }


    /**
     * The kinds of key fields that can be extracted from a
     * {@link GameActionInfo}.
     */
    public enum KeyField {
        TEMPERATURE {
            @Override
            public String extract(SensorData g) {
                return g.temperature.toString();
            }
        },
        DEVICEID {
            @Override
            public String extract(SensorData g) {
                return g.deviceId;
            }
        };

        public abstract String extract(SensorData g);
    }
}
