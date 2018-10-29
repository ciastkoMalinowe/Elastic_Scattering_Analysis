# Elastic_Scattering_Analysis
### Scala + Spark version 
Prometheus setup:
Load Spark module  
```   
     module load plgrid/apps/spark
```
Deploy Spark, if using Spark 2.3 (default) there is no starting script provided in $SPARK_HOME. I copied start_spark_cluster.sh from previous release
Command to run spark shell on Prometheus from Elastic_Scattering_Analysis directory with all dependencies and basic configuration:
```
spark-shell --packages org.diana-hep:spark-root_2.11:0.1.16,org.diana-hep:histogrammar-sparksql_2.11:1.0.3,org.diana-hep:histogrammar-bokeh_2.11:1.0.3,org.apache.commons:commons-math3:3.6.1 --conf spark.executor.memory=20g --conf spark.sql.codegen.wholeStage=false
```
#### Running script in spark-shell:
Loading all defining files:
```
     :load environment.scala
     :load kinematics.scala
     :load cuts.scala
     :load corrections.scala
```
Executing script:
```
     :paste script.scala
```

