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

query_file="bsbm_update_queries.txt"
if [ ! -f "${query_file}" ]; then
    >&2 echo "${query_file} not found."
    workspace_query_file='benchmark_workspace/benchmarks/bsbm/changesets/update_queries.txt'
    if [ ! -f "${workspace_query_file}" ]; then
        retrieving_query_file=1
        >&2 echo "also not present at ${workspace_query_file}. Downloading it."
        query_file_url="https://files.dice-research.org/datasets/hypertrie_update/bsbm/update_queries_bsbm100m.txt.zst"
        curl -L "${query_file_url}" | zstd -d > "${query_file}"
        >&2 echo "Downloaded and extracted ${query_file_url} to ${query_file}"
        retrieving_query_file=0
    else
        query_file="${workspace_query_file}"
    fi
fi
sha512sum -c <(echo "7db62dd159f7aab59500dd305b7e9e8133e15b4b529f79a4fe491d2d8862c7bd07bc88ba33bbc85acc868cb4e38145c59bf67dde9a37d7985527672206a8f702  ${query_file}")
>&2 echo "Reading update queries from ${query_file}"

output_file="tables/bsbm_update_sizes.csv"
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