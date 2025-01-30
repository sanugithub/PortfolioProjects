### Building Confluent Security Plugins

```shell
$ ./mvnw clean package -DskipTests
```

### About the Apache Maven wrapper

Development versions of confluent-security-plugins use version ranges for dependencies
on other Confluent projects, and due to
[a bug in Apache Maven](https://issues.apache.org/jira/browse/MRESOLVER-164),
you may find that both Maven and your IDE download hundreds or thousands
of pom files while resolving dependencies. They get cached, so it will
be most apparent on fresh forks or switching to branches that you haven't
used in a while.

Until we are able to get a fix in and released officially, we need to work
around the bug. We have added a Maven wrapper script and configured
it to download a patched version of Maven. You can use this wrapper simply by
substituting `./mvnw` instead of `mvn` for every Maven invocation.
The patch is experimental, so if it causes you some trouble, you can switch
back to the official release just by using `mvn` again. Please let us know
if you do have any trouble with the wrapper.

You can also configure IntelliJ IDEA to use the patched version of Maven.

First, get the path to the patched Maven home:

```shell
$ ./mvnw -v
...
Maven home: /home/john/.m2/wrapper/dists/apache-maven-3.8.1.2-bin/1npbma9t0n1k5b22fpopvupbmn/apache-maven-3.8.1.2
...
```

Then, update your IDEA Maven home in **Settings > Build, Execution, Deployment > Build Tools > Maven**
and set the value of **Maven home path** to the same directory listed as the "Maven home" above.
In the above example, it is `/home/john/.m2/wrapper/dists/apache-maven-3.8.1.2-bin/1npbma9t0n1k5b22fpopvupbmn/apache-maven-3.8.1.2`,
but it may be different on your machine.

There is likely a similar option available for other IDEs.