structured-content-tools
========================

This framework contains tools usefull to process/manipulate structured content represented in Java as `Map of Maps` structure. 
We use it to manipulate content before store it into ElasticSearch index.

Content manipulation can be done over chain of Preprocessors. Each preprocessor must implement `org.jboss.elasticsearch.tools.content.StructuredContentPreprocessor` interface.
You can use `org.jboss.elasticsearch.tools.content.StructuredContentPreprocessorBase` as base class for your preprocessor implementation.
Chain of preprocessors can be loaded using methods in `org.jboss.elasticsearch.tools.content.StructuredContentPreprocessorFactory`.

To simplify preprocessors implementation you can use methods from `org.jboss.elasticsearch.tools.content.ValueUtils` and `org.jboss.elasticsearch.tools.content.StructureUtils`.

Framework contains some generic configurable preprocessors implementation too:
* [AddValuePreprocessor](https://github.com/jbossorg/structured-content-tools/blob/master/src/main/java/org/jboss/elasticsearch/tools/content/AddValuePreprocessor.java) - allows to add value to some target field.
* [SimpleValueMapMapperPreprocessor](https://github.com/jbossorg/structured-content-tools/blob/master/src/main/java/org/jboss/elasticsearch/tools/content/SimpleValueMapMapperPreprocessor.java) - allows to perform mapping of simple value from source field over configured Map mapping structure to targed field. Optional default value can be used for values not found in mapping Map.

