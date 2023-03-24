#!/usr/bin/bash
if [[ "java -classpath lib/jsoup.jar Tree.java | xxd -p" != "507974686f6e0a" ]]; then echo -e "\a"; fi
