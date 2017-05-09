package StreamingDataFlow.testProject.util;

import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.Validation;

public interface CustomPipelineOptions extends DataflowPipelineOptions {
  @Description("ProjectId where data source topic lives")
  @Default.String("pubsub-public-data")
  @Validation.Required
  String getSourceProject();

  void setSourceProject(String value);

  @Description("TopicId of source topic")
  @Default.String("taxirides-realtime")
  @Validation.Required
  String getSourceTopic();

  void setSourceTopic(String value);

  @Description("ProjectId where data sink topic lives")
  @Validation.Required
  String getSinkProject();

  void setSinkProject(String value);

  @Description("TopicId of sink topic")
  @Default.String("visualizer")
  @Validation.Required
  String getSinkTopic();

  void setSinkTopic(String value);
}