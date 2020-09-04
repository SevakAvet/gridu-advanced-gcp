package com.sevak_avet;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method.STREAMING_INSERTS;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;

class LoadTweetsFromPubSub {

    private static final DateTimeFormatter DATETIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final Gson GSON = new GsonBuilder().registerTypeAdapter(LocalDateTime.class,
            (JsonDeserializer<LocalDateTime>) (json, type, jsonDeserializationContext) ->
                    LocalDateTime.parse(json.getAsJsonPrimitive().getAsString(), DATETIME_FORMAT)).create();

    public interface LoadTweetsFromPubSubOptions extends PipelineOptions {
        @Description("Comma separated string with keywords")
        @Default.String("trump,baiden")
        String getKeywords();

        void setKeywords(String topic);

        @Description("PubSub topic to read tweets from")
        @Default.String("projects/divine-bloom-279918/topics/tweets")
        String getInputTopic();

        void setInputTopic(String topic);

        @Description("PubSub topic to read tweets from")
        @Default.String("projects/divine-bloom-279918/subscriptions/tweets_subscription")
        String getSubscription();

        void setSubscription(String subscription);

        @Description("BQ table to write result to")
        @Default.String("tweets")
        String getOutputTable();

        void setOutputTable(String table);

    }

    public static class FilterMessages extends DoFn<Tweet, Tweet> {
        private final Logger LOG = LoggerFactory.getLogger(FilterMessages.class);
        private final String[] keywords;

        public FilterMessages(String[] keywords) {
            this.keywords = keywords;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            Tweet t = c.element();

            String text = t.getText().toLowerCase();
            long id = t.getId();

            LOG.info("text = {}", text);
            for (String keyword : this.keywords) {
                if (text.contains(keyword)) {
                    c.output(c.element());
                    LOG.info("Match: message = {}, keyword = {}, id = {}", text, keyword, id);
                }
            }
        }
    }

    public static final SimpleFunction<PubsubMessage, Tweet> pubSubMessageFormat = new SimpleFunction<PubsubMessage, Tweet>() {
        @Override
        public Tweet apply(PubsubMessage input) {
            String message = new String(input.getPayload(), StandardCharsets.UTF_8);
            return GSON.fromJson(message, Tweet.class);
        }
    };

    public static final SimpleFunction<Tweet, TableRow> formatFunction = new SimpleFunction<Tweet, TableRow>() {
        @Override
        public TableRow apply(Tweet t) {
            return new TableRow()
                    .set("author", t.getAuthor())
                    .set("place", t.getPlace())
                    .set("coordinates", t.getCoordinates())
                    .set("created_at", DATETIME_FORMAT.format(t.getCreatedAt()))
                    .set("text", t.getText())
                    .set("source", t.getSource())
                    .set("id", t.getId());
        }
    };


    public static void run(LoadTweetsFromPubSubOptions options) {
        Pipeline p = Pipeline.create(options);

        TableReference tableSpec = new TableReference()
                .setDatasetId("gridu")
                .setTableId(options.getOutputTable());

        TableSchema schema = new TableSchema().setFields(Arrays.asList(
                new TableFieldSchema().setName("author").setType("STRING").setMode("REQUIRED"),
                new TableFieldSchema().setName("place").setType("STRING").setMode("NULLABLE"),
                new TableFieldSchema().setName("coordinates").setType("STRING").setMode("NULLABLE"),
                new TableFieldSchema().setName("created_at").setType("DATETIME").setMode("REQUIRED"),
                new TableFieldSchema().setName("text").setType("STRING").setMode("REQUIRED"),
                new TableFieldSchema().setName("source").setType("STRING").setMode("REQUIRED"),
                new TableFieldSchema().setName("id").setType("INTEGER").setMode("REQUIRED")
        ));

        String[] keywords = options.getKeywords().split(",");
        p.apply("read tweets from pubsub",
                PubsubIO.readMessagesWithAttributes()
                        .fromSubscription(options.getSubscription()))
                .apply("Transform pubsub message to tweet", MapElements.via(pubSubMessageFormat))
                .apply("filter messaged by keywords", ParDo.of(new FilterMessages(keywords)))
                .apply("write to BQ", BigQueryIO.<Tweet>write()
                        .to(tableSpec)
                        .skipInvalidRows()
                        .withSchema(schema)
                        .withMethod(STREAMING_INSERTS)
                        .withFormatFunction(formatFunction)
                        .withCreateDisposition(CREATE_IF_NEEDED)
                        .withWriteDisposition(WRITE_APPEND));

        p.run().waitUntilFinish();
    }


    public static void main(String[] args) {
        LoadTweetsFromPubSubOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(LoadTweetsFromPubSubOptions.class);

        run(options);
    }
}