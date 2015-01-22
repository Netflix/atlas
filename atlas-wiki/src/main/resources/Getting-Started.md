## Run a Demo Instance

**Prerequisites**

* These instructions assume a unix based machine with [curl](http://curl.haxx.se/). Other systems may work, but have not been tried.
* Java 8 or higher is required.

To quickly run a version with some synthetic sample data:

```
$ curl -LO https://github.com/Netflix/atlas/releases/download/v1.4.1/atlas-1.4.1-standalone.jar
$ java -jar atlas-1.4.1-standalone.jar
```

## Explore Available Tags

The tags API is used to explore available tags and the relationships between them.

```
# show all tags
$ curl -s 'http://localhost:7101/api/v1/tags'

# show all values of the name, nf.app and type tags
$ curl -s 'http://localhost:7101/api/v1/tags/name'
$ curl -s 'http://localhost:7101/api/v1/tags/nf.app'
$ curl -s 'http://localhost:7101/api/v1/tags/type'

# show all name tags that also have the type tag
$ curl -s 'http://localhost:7101/api/v1/tags/name?q=type,:has'

# show all name tags that have an nf.app tag with a value of nccp
$ curl -s 'http://localhost:7101/api/v1/tags/name?q=nf.app,nccp,:eq'
```

## Generate Graphs

These graph API URLs show off a couple of the capabilities of the Atlas backend.  See the [Examples](https://github.com/Netflix/atlas/wiki/Examples) page for more detailed use cases.

```
# graph all metrics with a name tag value of ssCpuUser, using an :avg aggregation
$ curl -Lo graph.png 'http://localhost:7101/api/v1/graph?q=name,ssCpuUser,:eq,:avg'

# duplicate the ssCpuUser signal, check if it is greater than 22.8 and display the result as a vertical span with 30% alpha
$ curl -Lo graph.png 'http://localhost:7101/api/v1/graph?q=name,ssCpuUser,:eq,:avg,:dup,22.8,:gt,:vspan,30,:alpha'
```

## What's Next?

We still have some work to do to get it usable externally. The main part of this is planned for the next version with a rough ETA for the end of January 2015. 
