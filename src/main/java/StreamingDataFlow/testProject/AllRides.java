package StreamingDataFlow.testProject;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.TableRowJsonCoder;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.FlatMapElements;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import StreamingDataFlow.testProject.util.CustomPipelineOptions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class AllRides {
  private static final Logger LOG = LoggerFactory.getLogger(AllRides.class);

  // ride format from PubSub
  // {
  // "ride_id":"a60ba4d8-1501-4b5b-93ee-b7864304d0e0",
  // "latitude":40.66684000000033,
  // "longitude":-73.83933000000202,
  // "timestamp":"2016-08-31T11:04:02.025396463-04:00",
  // "meter_reading":14.270274,
  // "meter_increment":0.019336415,
  // "ride_status":"enroute" / "pickup" / "dropoff"
  // "passenger_count":2
  // }

  private static class PassThroughAllRides extends DoFn<TableRow, TableRow> {
    PassThroughAllRides() {}

    @Override
    public void processElement(ProcessContext c) throws IOException {
     	    TableRow obj = c.element();
	       float lat = Float.parseFloat(ride.get("latitude").toString());
       float lon = Float.parseFloat(ride.get("longitude").toString());
	    int pc = Integer.parseInt(obj.get("passenger_count").toString());
	           float mr = Float.parseFloat(ride.get("meter_reading").toString());
       float mi = Float.parseFloat(ride.get("meter_increment").toString());

	    TableRow ride =new TableRow().set("ride_id", obj.get("ride_id").toString).set("latitude",lat).set("longitude",lon)
		    .set("timestamp",obj.get("timestamp").toString()).set("ride_status" , obj.get("ride_status"))
		    .set("passenger_count",pc).set("meter_increment",mi).set("meter_reading",mr);
      
      // Access to data fields:
    
      
      c.output(ride);
    }
  }

  public static void main(String[] args) {
    CustomPipelineOptions options = PipelineOptionsFactory.create().as(CustomPipelineOptions.class);
    options.setRunner(DataflowPipelineRunner.class);
	options.setProject("healthcare-12");
	options.setStagingLocation("gs://mihin-data/staging12");
	options.setSinkProject("healthcare-12");
	options.setStreaming(true);
	options.setNumWorkers(3);
	options.setZone("europe-west1-c");
    Pipeline p = Pipeline.create(options);
    List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName("ride_id").setType("STRING"));
    fields.add(new TableFieldSchema().setName("passenger_count").setType("INTEGER"));
    fields.add(new TableFieldSchema().setName("ride_status").setType("STRING"));
    fields.add(new TableFieldSchema().setName("timestamp").setType("STRING"));
    fields.add(new TableFieldSchema().setName("longitude").setType("STRING"));
    fields.add(new TableFieldSchema().setName("meter_increment").setType("FLOAT"));
    fields.add(new TableFieldSchema().setName("meter_reading").setType("FLOAT"));
    fields.add(new TableFieldSchema().setName("latitude").setType("STRING"));


    TableSchema schema = new TableSchema().setFields(fields);
    p.apply(PubsubIO.Read.named("read from PubSub")
        .topic(String.format("projects/%s/topics/%s", options.getSourceProject(), options.getSourceTopic()))
        .timestampLabel("ts")
        .withCoder(TableRowJsonCoder.of()))

     // A Parallel Do (ParDo) transforms data elements one by one.
     // It can output zero, one or more elements per input element.
     .apply("pass all rides through 1", ParDo.of(new PassThroughAllRides()))

     // In Java 8 you can also use a simpler syntax through MapElements.
     // MapElements allows a single output element per input element.
//     .apply("pass all rides through 2",
//        MapElements.via((TableRow e) -> e).withOutputType(TypeDescriptor.of(TableRow.class)))

     // In java 8, if you need to return zero one or more elements per input, you can use
     // the FlatMapElements syntax. It expects you to return an iterable and will
     // gather all of its values into the output PCollection.
//     .apply("pass all rides through 3",
//        FlatMapElements.via(
//          (TableRow e) -> {
//            List<TableRow> a = new ArrayList<>();
//              a.add(e);
//              return a;
//            }).withOutputType(TypeDescriptor.of(TableRow.class)))

     //.apply(PubsubIO.Write.named("write to PubSub").topic(String.format("projects/%s/topics/%s", options.getSinkProject(), options.getSinkTopic())).withCoder(TableRowJsonCoder.of()));
     .apply(BigQueryIO.Write.named("Writeing to Big Querry").to("healthcare-12:wordcount_dataset.streamingTest123")
    		 .withSchema(schema)
    	      .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
    	      .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
     p.run();
  }
}
