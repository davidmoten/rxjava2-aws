#!/bin/bash
set -e
PROJECT=rxjava2-aws
mvn site
cd ../davidmoten.github.io
git pull
mkdir -p $PROJECT
cp -r ../$PROJECT/target/site/* $PROJECT/
git add .
git commit -am "update site reports"
git push
