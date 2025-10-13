#!/bin/bash

set -e

neo4j="${PWD}/$1"
input=$2

export NEO4J_HOME=$neo4j
export NEO4J_IMPORT=${NEO4J_HOME}"/import"

ulimit -n 65535

echo "Creating index"
${NEO4J_HOME}/bin/cypher-shell -u neo4j -p 'jazero_admin' "CREATE CONSTRAINT n10s_unique_uri ON (r:Resource) ASSERT r.uri IS UNIQUE;"
${NEO4J_HOME}/bin/cypher-shell -u neo4j -p 'jazero_admin' 'call n10s.graphconfig.init( { handleMultival: "OVERWRITE",  handleVocabUris: "SHORTEN", keepLangTag: false, handleRDFTypes: "NODES" })'

echo
echo "Moving and cleaning"
rm -rf ${NEO4J_IMPORT}/*

for f in ${input}/* ; \
do
  FILE_CLEAN="$(basename "${f}")"
  iconv -f utf-8 -t ascii -c "${f}" | grep -E '^<(https?|ftp|file)://[-A-Za-z0-9\+&@#/%?=~_|!:,.;]*[A-Za-z0-9\+&@#/%?=~_|]>\W<' | grep -Fv 'xn--b1aew' > ${NEO4J_IMPORT}/${FILE_CLEAN}
done

echo "Importing..."

for f in ${NEO4J_IMPORT}/* ; \
do
  filename="$(basename ${f})"

  echo "importing ${filename} from ${NEO4J_IMPORT}"
  ${NEO4J_HOME}/bin/cypher-shell -u neo4j -p 'jazero_admin' "CALL  n10s.rdf.import.fetch(\"file://${NEO4J_IMPORT}/${filename}\",\"Turtle\");"
done

echo "Done"