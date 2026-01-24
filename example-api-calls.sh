#!/usr/bin/env bash
set -euo pipefail

BASE="http://localhost:8000"

echo "Q1: Total logs per type (HDFS window)"
curl -sG "$BASE/analytics/logs-per-type" \
  --data-urlencode "start=2008-11-09T00:00:00Z" \
  --data-urlencode "end=2008-11-11T23:59:59Z"
echo

echo "Q2: Requests per day for HDFS_DATAXCEIVER"
curl -sG "$BASE/analytics/requests-per-day" \
  --data-urlencode "logSet=HDFS_DATAXCEIVER" \
  --data-urlencode "start=2008-11-09T00:00:00Z" \
  --data-urlencode "end=2008-11-11T23:59:59Z"
echo

echo "Q3: Top 3 logs per source IP on 2008-11-09"
curl -sG "$BASE/analytics/top3-per-sourceip" \
  --data-urlencode "day=2008-11-09"
echo

echo "Q4: Two least common HTTP methods (Apache window)"
curl -sG "$BASE/analytics/least-http-methods" \
  --data-urlencode "start=2005-06-09T00:00:00Z" \
  --data-urlencode "end=2006-02-28T23:59:59Z"
echo

echo "Q5: Referrers leading to more than one resource"
curl -s "$BASE/analytics/referrers-multi-resource"
echo

echo "Q6: Blocks replicated and served on same day (2008-11-09)"
curl -sG "$BASE/analytics/blocks-replicated-and-served" \
  --data-urlencode "day=2008-11-09"
echo

echo "Q7: Top 50 upvoted logs on 2008-11-09"
curl -sG "$BASE/analytics/top-upvoted-logs" \
  --data-urlencode "day=2008-11-09"
echo

echo "Q8: Top 50 admins by total upvotes"
curl -s "$BASE/analytics/top-admins-upvotes"
echo

echo "Q9: Top 50 admins by number of source IPs voted"
curl -s "$BASE/analytics/top-admins-sourceips"
echo

echo "Q10: Logs where same email used with multiple usernames"
curl -s "$BASE/analytics/logs-multi-username-per-email"
echo

echo "Q11: Block IDs voted by a real username"
USERNAME="$(curl -s "$BASE/analytics/top-admins-upvotes" | jq -r '.results[0].username')"
curl -sG "$BASE/analytics/blockids-voted" \
  --data-urlencode "username=$USERNAME"
echo
