#!/bin/bash

set -x

hadoop com.sun.tools.javac.Main GetTitle.java
jar cf GetTitle.jar GetTitle*.class
