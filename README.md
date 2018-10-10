# Elastic_Scattering_Analysis
### Scala + Spark version 
Commands to setup environment on HN worker node:  
```   
     source /cvmfs/sft.cern.ch/lcg/views/LCG_93/x86_64-slc6-gcc62-opt/setup.sh 
     source /cvmfs/sft.cern.ch/lcg/etc/hadoop-confext/hadoop-setconf.sh totemhn
```
Command to run spark shell from Elastic_Scattering_Analysis directory with all dependencies and basic configuration:
```
spark-shell --master yarn --packages org.diana-hep:spark-root_2.11:0.1.16,org.diana-hep:histogrammar-sparksql_2.11:1.0.3,org.diana-hep:histogrammar-bokeh_2.11:1.0.3 --conf spark.authenticate.enableSaslEncryption=true --conf spark.authenticate=true --conf spark.executorEnv.LD_LIBRARY_PATH=$LD_LIBRARY_PATH --conf spark.extraListeners= "" --conf spark.executor.memory=20g
```
#### Running script in spark-shell:
Loading all defining files:
```
     :load environment.scala
     :load kinematics.scala
     :load cuts.scala
```
Executing script:
```
     :paste script.scala
```

