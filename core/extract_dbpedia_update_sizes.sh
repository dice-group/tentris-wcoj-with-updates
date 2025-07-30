#!/bin/bash

set -euo pipefail

retrieving_query_file=0

abort()
{
    if [ $retrieving_query_file -eq 1 ]; then
        rm "${query_file}"
    fi
    >&2 echo  'An error occurred. Exiting ...'
    exit 1
}

trap 'abort' 0

query_file="dbpedia_update_queries.txt"
if [ ! -f "${query_file}" ]; then
    >&2 echo "${query_file} not found."
    workspace_query_file='benchmark_workspace/benchmarks/dbpedia/changesets/update_queries.txt'
    if [ ! -f "${workspace_query_file}" ]; then
        retrieving_query_file=1
        >&2 echo "also not present at ${workspace_query_file}. Downloading it."
        query_file_url="https://files.dice-research.org/datasets/hypertrie_update/dbpedia/dbpedia-2015-10-changelog-queries.zip"
        wget "${query_file_url}"
        unzip dbpedia-2015-10-changelog-queries.zip
        mv test-queries.txt "${query_file}"
        >&2 echo "Downloaded and extracted ${query_file_url} to ${query_file}"
        retrieving_query_file=0
    else
        query_file="${workspace_query_file}"
    fi
fi
sha512sum -c <(echo "ce7ff4a6fd092a7c124faba76dd6be3ede8b5b06835abda4a4262b9678bc88132ed2a14934d6590e151a6585d3a93f36e904358bef77c714761e816a736337be  ${query_file}")
>&2 echo "Reading update queries from ${query_file}"

output_file="tables/dbpedia_update_sizes.csv"
>&2 echo "Writing to ${output_file}"
echo -n '' > $output_file
# iterate over the update queries
qid=0
echo "query_id,update_size_triples,update_size_bytes" >> $output_file
while read -r query; do
    # get the number of triples
    size_in_triples=$(echo "$query" |  grep -Fo " . " | wc -l)
    # get the size in bytes
    size_in_bytes=${#query}
    # remove the bytes for the strings "INSERT DATA {"or "DELETE DATA {" and the trailing character "}" (14 characters in total)
    size_in_bytes=$((size_in_bytes - 14))
    # print the stats
    echo "$qid,$size_in_triples,$size_in_bytes" >> $output_file
    # increment counter
    qid=$((qid + 1))
    if [ $((qid % 1000)) -eq 0 ]; then
        >&2 echo "Processed $qid queries."
    fi
done < "${query_file}"

>&2 echo "Processed $qid queries."

trap : 0
