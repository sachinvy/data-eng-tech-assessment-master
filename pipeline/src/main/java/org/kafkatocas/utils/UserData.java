package org.json2kafka.utils;

import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldName;

import javax.annotation.Nullable;

public class UserData {
    @DefaultSchema(AutoValueSchema.class)
    @AutoValue
    public abstract static class UserDataEvent {
        public static Builder builder() {
            return new AutoValue_UserData_UserDataEvent.Builder();
        }

        @SchemaFieldName("timestamp")
        public abstract String getTimestamp();

        @SchemaFieldName("locationid")
        public abstract String getLocationId();

        @SchemaFieldName("direction_1")
        public abstract String getDirection1();

        @SchemaFieldName("direction_2")
        public abstract String getDirection2();

        @SchemaFieldName("total_of_directions")
        public abstract String getTotalOfDirections();

        public abstract Builder toBuilder();

        @AutoValue.Builder
        public abstract static class Builder {

            public abstract Builder setTimestamp(String value);

            public abstract Builder setLocationId(String value);

            public abstract Builder setDirection1(String value);

            public abstract Builder setDirection2(String value);

            public abstract Builder setTotalOfDirections(String value);

            public abstract UserDataEvent build();
        }
    }

    @DefaultSchema(AutoValueSchema.class)
    @AutoValue
    public abstract static class UserDataReference {
        public static Builder builder() {
            return new AutoValue_UserData_UserDataReference.Builder();
        }

        @SchemaFieldName("timestamp")
        public abstract String getTimestamp();

        @SchemaFieldName("locationid")
        public abstract String getLocationId();

        @SchemaFieldName("direction_1")
        public abstract String getDirection1();

        @SchemaFieldName("direction_2")
        public abstract String getDirection2();

        @SchemaFieldName("total_of_directions")
        public abstract String getTotalOfDirections();

        public abstract Builder toBuilder();

        @AutoValue.Builder
        public abstract static class Builder {

            public abstract Builder setTimestamp(String value);

            public abstract Builder setLocationId(String value);

            public abstract Builder setDirection1(String value);

            public abstract Builder setDirection2(String value);

            public abstract Builder setTotalOfDirections(String value);

            public abstract UserDataReference build();
        }
    }
}

