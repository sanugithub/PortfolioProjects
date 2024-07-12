# ce-kafka-rest

kafka-rest wrapper implementing ce-kafka's KafkaHttpServlet provider interface 

### Building against a specific ce-kafka version

You can specify the version used for the ce-kafka dependencies of ce-kafka-rest by adding `-Dce.kafka.version=<version>` to the Maven build script you are using.
- E.g. `mvn clean package -Dce.kafka.version=6.1.2-42-ce`.

This also allows you to build against custom/local ce-kafka changes by doing the following:
1. Implement/check out locally the ce-kafka changes you want to build against.
2. Follow ce-kafka's [instructions for publishing to your local Maven repo](https://github.com/confluentinc/ce-kafka#installing-the-jars-to-the-local-maven-repository).
   1. You might also have to specify `mavenUrl=file://<path to local Maven repo>` in your `gradle.properties`.
3. Verify that you can see the newly created artifacts (which should use 0 as a nanoversion) in your local repo.
4. Build ce-kafka-rest with `-Dce.kafka.version` set to the locally published version.
   1. E.g. `mvn clean package -Dce.kafka.version=6.1.2-0-ce`.

### Building against a specific kafka-rest version

You can specify the version used for the kafka-rest dependencies of ce-kafka-rest by adding  `-Dio.confluent.kafka.rest.version=<version>` to the Maven build script you are using.
- E.g ` mvn clean package -Dio.confluent.kafka.rest.version=7.3.0-ce-0`

This also allows you to build against custom/local kafka-rest changes by doing the following:
1. Implement/check out locally the kafka-rest changes you want to build against.
2. Ensure that the changes are in your local Maven repo by running `mvn install`.  This will create a -0 nano-versioned instance of kafka-rest in your Maven repository.
   1. Check you can see the changes in your local Maven repository by looking in `~/.m2/repository/io/confluent/kafka-rest` for a -0 version of kafka-rest. E.g. `~/.m2/repository/io/confluent/kafka-rest/7.3.0-0`
3. Update `ce-kafka-rest-extensions/pom.xml` and `ce-kafka-rest-servlet/pom.xml` to replace the kafka-rest versions, which are `${io.confluent.kafka-rest.version}` with the version you want to use, eg `7.3.0-0`
4. Build ce-kafka-rest with the `-Dio.confluent.kafka.rest.version` set to the locally published version of kafka-rest.  E.g. `mvn clean package -Dio.confluent.kafka.rest.version=7.3.0-ce-0`.
5. Update the appropriate Dockerfile to copy the local kafka-rest jar into the running image.
   1. Copy the built `kafka-rest/target/kafka-rest-7.3.0-0-package/share/java/kafka-rest-lib/kafka-rest-7.3.0-0.jar` from the kafka-rest repository to eg `ce-kafka-rest-extensions/target/ce-kafka-rest-extensions-7.3.0-0-package/kafka-rest-7.3.0-0.jar` so that the Dockerfile can access it.
   2. Add `COPY ce-kafka-rest-extensions/target/ce-kafka-rest-extensions-7.3.0-0-package/kafka-rest-7.3.0-0.jar /usr/bin/../share/java/kafka-rest-lib/kafka-rest-7.3.0-0.jar` to the Dockerfile that your testing environment uses to ensure the updated kafka-rest classes are present in the classpath. For example `testing/environments/standalone-cloud/run.sh` uses `testing/images/kafka-rest/Dockerfile`
6. Run the test environment using ./run.sh
   1. Remember to either comment out or update the mvn package step to point at the new kafka-rest version
7. You can also pass additional command line options to the REST instance by using the KAFKAREST_OPTS environment variable, which you can add to the docker-compose.yaml. E.g.   `KAFKAREST_OPTS: "-Dcom.sun.management.jmxremote.port=1616"`
