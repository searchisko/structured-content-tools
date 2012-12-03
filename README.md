structured-content-tools
========================

This framework contains tools usefull to process/manipulate structured content represented in Java as [`Map of Maps` structure](http://wiki.fasterxml.com/JacksonInFiveMinutes#A.22Raw.22_Data_Binding_Example). 
We use it to allow highly configurable manipulation with content before store into ElasticSearch search index, for example in [JIRA River Plugin for ElasticSearch](https://github.com/jbossorg/elasticsearch-river-jira).

Content manipulation can be done over chain of Preprocessors. Each preprocessor must implement [`org.jboss.elasticsearch.tools.content.StructuredContentPreprocessor`](https://github.com/jbossorg/structured-content-tools/blob/master/src/main/java/org/jboss/elasticsearch/tools/content/StructuredContentPreprocessor.java) interface.
You can use [`org.jboss.elasticsearch.tools.content.StructuredContentPreprocessorBase`](https://github.com/jbossorg/structured-content-tools/blob/master/src/main/java/org/jboss/elasticsearch/tools/content/StructuredContentPreprocessorBase.java) as base class for your preprocessor implementation.
Chain of preprocessors can be loaded using methods in [`org.jboss.elasticsearch.tools.content.StructuredContentPreprocessorFactory`](https://github.com/jbossorg/structured-content-tools/blob/master/src/main/java/org/jboss/elasticsearch/tools/content/StructuredContentPreprocessorFactory.java).

To simplify preprocessors implementation you can use methods from [`org.jboss.elasticsearch.tools.content.ValueUtils`](https://github.com/jbossorg/structured-content-tools/blob/master/src/main/java/org/jboss/elasticsearch/tools/content/ValueUtils.java) and [`org.jboss.elasticsearch.tools.content.StructureUtils`](https://github.com/jbossorg/structured-content-tools/blob/master/src/main/java/org/jboss/elasticsearch/tools/content/StructureUtils.java).

Framework contains some generic configurable preprocessors implementation too:
* [`AddValuePreprocessor`](https://github.com/jbossorg/structured-content-tools/blob/master/src/main/java/org/jboss/elasticsearch/tools/content/AddValuePreprocessor.java) - allows to add value to some target field. Value can be constant or contain pattern with keys for replacement with other data from content.
* [`AddMultipleValuesPreprocessor`](https://github.com/jbossorg/structured-content-tools/blob/master/src/main/java/org/jboss/elasticsearch/tools/content/AddMultipleValuesPreprocessor.java) - allows to add multiple value to some target fields. Value can be constant or contain pattern with keys for replacement with other data from content.
* [`AddCurrentTimestampPreprocessor`](https://github.com/jbossorg/structured-content-tools/blob/master/src/main/java/org/jboss/elasticsearch/tools/content/AddCurrentTimestampPreprocessor.java) - allows to add current timestamp to some target field.
* [`SimpleValueMapMapperPreprocessor`](https://github.com/jbossorg/structured-content-tools/blob/master/src/main/java/org/jboss/elasticsearch/tools/content/SimpleValueMapMapperPreprocessor.java) - allows to perform mapping of simple value from source field over configured Map mapping structure to targed field. Optional default value can be used for values not found in mapping Map.
* [`ValuesCollectingPreprocessor`](https://github.com/jbossorg/structured-content-tools/blob/master/src/main/java/org/jboss/elasticsearch/tools/content/ValuesCollectingPreprocessor.java) - collects values from multiple source fields (some of them can contain lists), remove duplicities, and store values as List in target field.
* [`ESLookupValuePreprocessor`](https://github.com/jbossorg/structured-content-tools/blob/master/src/main/java/org/jboss/elasticsearch/tools/content/ESLookupValuePreprocessor.java) - uses defined value from data to lookup another value in ElasticSearch search index and put result into defined target field in data. 

jar is available over [JBoss maven repository](https://community.jboss.org/docs/DOC-15169), you can use this dependency snippet in your `pom.xml`:

	<dependency>
	  <groupId>org.jboss.elasticsearch</groupId>
	  <artifactId>structured-content-tools</artifactId>
	  <version>1.0.0</version>
	</dependency>

