
Motivations

We, along with other academic institutions, have used Torque PBS for many years to run a variety of research workloads. However, with the increasing prevalence of big data technologies, the Hadoop stack has become more prevalent, and encapsulate a range of attractive tools. 

Can we mix PBS and Hadoop environments?

Early versions of Hadoop supported Hadoop-on-Demand (https://hadoop.apache.org/docs/r1.2.1/hod_scheduler.html), aka HOD, the ability to bring up a Hadoop cluster upon a Torque or Sun Grid Engine cluster. HOD has been deprecated and removed within Hadoop 2.0. MyHadoop (https://github.com/glennklockwood/myhadoop/) is a similar initiative, but which only supports Hadoop 1.0.

Apart from being outdated, the problem with both the HOD and MyHadoop initiatives is that they take existing clusters and add Hadoop-on-top. This ends up dedicating resources to single Hadoop jobs, instead of sharing them across jobs. When a job does not use all resources allocated (e.g. when a MapReduce job is ending) this is particularly wasteful of resources.

Why ProBoS?

ProBoS offers an alternative architecture, by offering PBS functionality upon a Hadoop YARN cluster. This means that PBS, MapReduce, Spark etc., jobs can co-exist on the same cluster. Users familiar with PBS commands such as qsub, qstat can continue to run their jobs in the classical way. 

We believe that, with increasing maturity of ProBoS, this means that Torque clusters can be changed over to Hadoop to facilitate mixed workloads, while easily permitting access to new Hadoop technologies on the same clusters.
