#!/bin/bash

set -x

hadoop com.sun.tools.javac.Main GraphBuilder.java
jar cf GraphBuilder.jar GraphBuilder*.class
