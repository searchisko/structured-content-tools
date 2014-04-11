structured-content-tools
========================

This framework contains tools useful to process/manipulate structured content 
represented in Java as [`Map of Maps` structure](http://wiki.fasterxml.com/JacksonInFiveMinutes#A.22Raw.22_Data_Binding_Example). 
This structure is used often to represent variable JSON data. 

We use this framework to allow highly configurable manipulation with content before store 
into ElasticSearch search index, for example in [JIRA River Plugin for 
ElasticSearch](https://github.com/jbossorg/elasticsearch-river-jira) 
or [DCP platform](https://github.com/jbossorg/dcp-api).

Content manipulation is performed over chain of Preprocessors. Each preprocessor 
must implement [`org.jboss.elasticsearch.tools.content.StructuredContentPreprocessor`](src/main/java/org/jboss/elasticsearch/tools/content/StructuredContentPreprocessor.java) 
interface.
You can use [`org.jboss.elasticsearch.tools.content.StructuredContentPreprocessorBase`](src/main/java/org/jboss/elasticsearch/tools/content/StructuredContentPreprocessorBase.java) 
as base class for your preprocessor implementation.
Chain of preprocessors can be loaded using methods in 
[`org.jboss.elasticsearch.tools.content.StructuredContentPreprocessorFactory`](src/main/java/org/jboss/elasticsearch/tools/content/StructuredContentPreprocessorFactory.java).

You can use methods from 
[`org.jboss.elasticsearch.tools.content.ValueUtils`](src/main/java/org/jboss/elasticsearch/tools/content/ValueUtils.java) 
and [`org.jboss.elasticsearch.tools.content.StructureUtils`](src/main/java/org/jboss/elasticsearch/tools/content/StructureUtils.java) to simplify preprocessors implementation.

Framework contains some generic configurable preprocessors implementation:

* [`AddValuePreprocessor`](src/main/java/org/jboss/elasticsearch/tools/content/AddValuePreprocessor.java) - 
  allows to add value to some target field. Value can be constant or contain 
  pattern with keys for replacement with other data from content.
* [`AddMultipleValuesPreprocessor`](src/main/java/org/jboss/elasticsearch/tools/content/AddMultipleValuesPreprocessor.java) - 
  allows to add multiple value to some target fields. Value can be constant 
* [`RemoveMultipleFieldsPreprocessor`](src/main/java/org/jboss/elasticsearch/tools/content/RemoveMultipleFieldsPreprocessor.java) - 
  allows to remove one or more fields from data structure.
* [`AddCurrentTimestampPreprocessor`](src/main/java/org/jboss/elasticsearch/tools/content/AddCurrentTimestampPreprocessor.java) - 
  allows to add current timestamp to some target field.
* [`SimpleValueMapMapperPreprocessor`](src/main/java/org/jboss/elasticsearch/tools/content/SimpleValueMapMapperPreprocessor.java) - 
  allows to perform mapping of simple value from source field over configured 
  Map mapping structure to targed field. Optional default value can be used 
  for values not found in mapping Map.
* [`ValuesCollectingPreprocessor`](src/main/java/org/jboss/elasticsearch/tools/content/ValuesCollectingPreprocessor.java) - 
  collects values from multiple source fields (some of them can contain lists), 
  remove duplicities, and store values as List in target field.
* [`ESLookupValuePreprocessor`](src/main/java/org/jboss/elasticsearch/tools/content/ESLookupValuePreprocessor.java) - 
  uses defined value from data to lookup document in ElasticSearch search index and 
  put defined fields from it into defined target fields in data.
* [`MaxTimestampPreprocessor`](src/main/java/org/jboss/elasticsearch/tools/content/MaxTimestampPreprocessor.java) - 
  selects max timestamp value from array in source field and store it into target field
* [`RequiredValidatorPreprocessor`](src/main/java/org/jboss/elasticsearch/tools/content/RequiredValidatorPreprocessor.java) - 
  checks defined source field for 'required' condition and throws exception if not match
* [`TrimStringValuePreprocessor`](src/main/java/org/jboss/elasticsearch/tools/content/TrimStringValuePreprocessor.java) - 
  trim String value from source field to the configured maximal length (whitespaces at the beginning and end are removed too) and store it into target field
* [`StripHtmlPreprocessor`](src/main/java/org/jboss/elasticsearch/tools/content/StripHtmlPreprocessor.java) - 
  strip HTML tags and unescape HTML entities from String value of source field and store it into target field

structured-content-tools jar file is available from [JBoss maven 
repository](https://community.jboss.org/docs/DOC-15169), you can use this 
dependency snippet in your `pom.xml`.

For Elasticsearch 1.0.x series and java 1.7

	<dependency>
	  <groupId>org.jboss.elasticsearch</groupId>
	  <artifactId>structured-content-tools</artifactId>
	  <version>1.3.0</version>
	</dependency>

For Elasticsearch 0.90.5 series and java 1.6

	<dependency>
	  <groupId>org.jboss.elasticsearch</groupId>
	  <artifactId>structured-content-tools</artifactId>
	  <version>1.2.8</version>
	</dependency>
	