#!/usr/bin/env sh

mvn clean
mvn compile
mvn package
cp target/tree-eclass-1.0-SNAPSHOT.jar prod/tree-eclass.jar
