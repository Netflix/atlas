
[Double exponential smoothing](http://www.itl.nist.gov/div898/handbook/pmc/section4/pmc433.htm)
(DES) is a simple technique for generating a smooth trend line from another time series. This
technique is often used to generate a dynamic threshold for alerting.

> :warning: Alerts on dynamic thresholds should be expected to be noisy. They are looking for strange behavior rather than an actual problem causing impact. Make sure you will actually spend the time to tune and investigate the alarms before using this approach. See the [alerting philosophy](Alerting-Philosophy) guide for more information on best practices.  

## Tuning

The [:des](Stack-Language-Reference#des) operator takes 4 parameters:

* An input time series
* `training` - the number of intervals to use for warming up before generating an output
* `alpha` - is a data smoothing factor
* `beta` - is a trend smoothing factor

### Training

The training parameter defines how many intervals to allow the DES to warmup. In the graph below the gaps from the start of the chart to the smooted lines reflects the training window used:

```wiki.script
graph.image("/api/v1/graph?q=:true,800,:fadd,input,:legend,:true,400,:fadd,30,0.1,1,:des,training+%3D+30,:legend,:true,90,0.1,1,:des,training+%3D+90,:legend,:list,(,nf.cluster,alerttest,:eq,name,requestsPerSecond,:eq,:and,:cq,),:each&l=0&s=e-2d", false)
```

Typically a training window of 10 has been sufficient as DES will adjust to the input fairly quick. 
However, in some cases if there is a massive change in the input it can cause DES to oscillate, for example:

```wiki.script
graph.image("/api/v1/graph?tz=UTC&e=2012-01-01T09:00&s=e-6h&w=750&h=150&q=nf.cluster,alerttest,:eq,name,requestsPerSecond,:eq,:and,:sum,:dup,10,0.1,0.5,:des,0.9,:mul,:2over,:lt,:rot,$name,:legend,:rot,prediction,:legend,:rot,:vspan,60,:alpha,alert+triggered,:legend", false)
```
 

### Alpha

Alpha is the data smoothing factor. A value of 1 means no smoothing. The closer the value gets to 0 the smoother the line should get. Example:

```wiki.script
graph.image("/api/v1/graph?q=:true,1200,:fadd,input,:legend,:true,10,1,1,:des,800,:fadd,alpha+%3D+1,:legend,:true,10,0.1,1,:des,400,:fadd,alpha+%3D+0.1,:legend,:true,10,0.01,1,:des,alpha+%3D+0.01,:legend,:list,(,nf.cluster,alerttest,:eq,name,requestsPerSecond,:eq,:and,:cq,),:each&l=0&s=e-2d", false)
```

### Beta

Beta is a trend smoothing factor. Visually it is most apparent when alpha is small. Example with `alpha = 0.01`:

```wiki.script
graph.image("/api/v1/graph?q=:true,1200,:fadd,input,:legend,:true,10,0.01,1,:des,800,:fadd,beta+%3D+1,:legend,:true,10,0.01,0.1,:des,400,:fadd,beta+%3D+0.1,:legend,:true,10,0.01,0.01,:des,beta+%3D+0.01,:legend,:list,(,nf.cluster,alerttest,:eq,name,requestsPerSecond,:eq,:and,:cq,),:each&l=0&s=e-2d", false)
```

## Recommended Values

Experimentally we have converged on 3 sets of values based on how quickly it should adjust to changing levels in the input signal.

| Helper                                             | Alpha  | Beta  |
|----------------------------------------------------|--------|-------|
| [:des-fast](Stack-Language-Reference#des-fast)     | 0.1    | 0.02  |
| [:des-slower](Stack-Language-Reference#des-slower) | 0.05   | 0.03  |
| [:des-slow](Stack-Language-Reference#des-slow)     | 0.03   | 0.04  |


Here is an example of how they behave for a sharp drop and recovery:

```wiki.script
graph.image("/api/v1/graph?tz=UTC&e=2012-01-01T09:00&s=e-6h&w=750&h=150&q=:true,input,:legend,:true,:des-fast,des-fast,:legend,:true,:des-slower,des-slower,:legend,:true,:des-slow,des-slow,:legend,:list,(,nf.cluster,alerttest,:eq,name,requestsPerSecond,:eq,:and,:cq,),:each", false)
```

For a more gradual drop:

```wiki.script
graph.image("/api/v1/graph?tz=UTC&e=2012-01-02T09:00&s=e-6h&w=750&h=150&q=:true,input,:legend,:true,:des-fast,des-fast,:legend,:true,:des-slower,des-slower,:legend,:true,:des-slow,des-slow,:legend,:list,(,nf.cluster,alerttest,:eq,name,requestsPerSecond,:eq,:and,:cq,),:each&l=0", false)
```

If the drop is smooth enough then DES can adjust without ever triggering.

## Alerting

For alerting purposes the DES line will typically get multiplied by a fraction and then checked to see whether the input line drops below the DES value for a given interval. 

```
# Query to generate the input line
nf.cluster,alerttest,:eq,
name,requestsPerSecond,:eq,:and,
:sum,

# Create a copy on the stack
:dup,

# Apply a DES function to generate a prediction
:des-fast,

# Used to set a threshold. The prediction should
# be roughly equal to the line, in this case the
# threshold would be 85% of the prediction.
0.85,:mul,

# Create a boolean signal line that is 1
# for datapoints where the actual value is
# less than the prediction and 0 where it
# is greater than or equal the prediction.
# The 1 values are where the alert should
# trigger.
:lt,

# Apply presentation details.
:rot,$name,:legend,
```

The vertical spans show when the expression would have triggered with due to the input dropping below the DES line at 85%:

```wiki.script
graph.image("/api/v1/graph?tz=UTC&e=2012-01-01T09:00&s=e-6h&w=750&h=150&q=:true,:true,0.85,:mul,:des-fast,:2over,:lt,:vspan,40,:alpha,:rot,input,:legend,:rot,des-fast,:legend,:rot,triggered,:legend,:list,(,nf.cluster,alerttest,:eq,name,requestsPerSecond,:eq,:and,:cq,),:each&l=0", false)
```

```wiki.script
graph.image("/api/v1/graph?tz=UTC&e=2012-01-02T09:00&s=e-6h&w=750&h=150&q=:true,:true,0.85,:mul,:des-fast,:2over,:lt,:vspan,40,:alpha,:rot,input,:legend,:rot,des-fast,:legend,:rot,triggered,:legend,:list,(,nf.cluster,alerttest,:eq,name,requestsPerSecond,:eq,:and,:cq,),:each&l=0", false)
```

