# SINDY

SINDY is a *S*calable *IN*clusion *D*ependenc*Y* algorithm based on Apache Flink described in a [BTW'15 paper](https://hpi.de/fileadmin/user_upload/fachgebiete/naumann/publications/2015/Scaling_out_the_discovery_of_INDs-CR.pdf).
In contrast to the original algorithm in the publication, this repository contains derivatives of SINDY that support n-ary IND discovery and approximate/partial IND discovery.

## Getting started

SINDY is written in Java 8 and can be built using Maven 3.*. Although SINDY is based on Apache Flink, you don't necessarily need a cluster to run it (but you can). Flink is included as a library and will still use core-parallelism on single machines.

There are basically three options to run SINDY:
* You can integrate it as a library into your code (see the module `sindy-core`).
* You can use SINDY as a [Metanome](https://github.com/HPI-Information-Systems/Metanome.git) algorithm (see the module `sindy-metanome`). Running Maven's `package` lifecycle phase, a Metanome algorithm jar file will be built. _Note that Metanome currently does not support partial INDs._
* You can run SINDY together with the [Metadata Management System](https://github.com/stratosphere/metadata-ms) for further analysis of the discovered INDs (see the module `sindy-mdms`). Running Maven's `package` lifecycle phase, a small distribution for the algorithm will be built. _Note that currently, MDMS does not support partial INDs, either._
