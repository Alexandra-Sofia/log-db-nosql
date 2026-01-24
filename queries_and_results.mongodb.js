use("nosql_logs");

/* ============================================================
   Q1. Total logs per actionType in a time range (descending)
   Time range: 2008-11-09 .. 2008-11-11 (UTC)
============================================================ */
print("\nQ1: Total logs per actionType (2008-11-09..2008-11-11)");
db.logs.aggregate([
    { $match: { ts: { $gte: ISODate("2008-11-09T00:00:00Z"), $lte: ISODate("2008-11-11T23:59:59Z") } } },
    { $group: { _id: "$actionType", total: { $sum: 1 } } },
    { $sort: { total: -1 } }
]).toArray();
// results
resultsQ1 = [
    {
        "_id": "receiving",
        "total": 1723232
    },
    {
        "_id": "update",
        "total": 1719741
    },
    {
        "_id": "served",
        "total": 428726
    },
    {
        "_id": "replicate",
        "total": 7167
    },
    {
        "_id": "received",
        "total": 7097
    }
];

/* ============================================================
   Q2. Total requests per day for a logSet and time range
   logSet: HDFS_DATAXCEIVER
   Time range: 2008-11-09 .. 2008-11-11 (UTC)
============================================================ */
print("\nQ2: Requests per day (HDFS_DATAXCEIVER, 2008-11-09..2008-11-11)");
db.logs.aggregate([
    { $match: { logSet: "HDFS_DATAXCEIVER", ts: { $gte: ISODate("2008-11-09T00:00:00Z"), $lte: ISODate("2008-11-11T23:59:59Z") } } },
    { $group: { _id: "$day", total: { $sum: 1 } } },
    { $sort: { _id: 1 } }
]).toArray();
// results
resultsQ2 = [
    {
        "_id": "2008-11-09",
        "total": 204925
    },
    {
        "_id": "2008-11-10",
        "total": 1040154
    },
    {
        "_id": "2008-11-11",
        "total": 913976
    }
];


/* ============================================================
   Q3. Three most common logs per source IP for a day
   Day: 2008-11-09
============================================================ */
print("\nQ3: Top 3 logs per source IP (day=2008-11-09)");
db.logs.aggregate([
    { $match: { day: "2008-11-09", sourceIp: { $ne: null } } },
    {
        $addFields: {
            sig: {
                $cond: [
                    { $eq: ["$logSet", "ACCESS"] },
                    {
                        $concat: [
                            { $ifNull: ["$access.method", ""] },
                            " ",
                            { $ifNull: ["$access.resource", ""] }
                        ]
                    },
                    { $ifNull: ["$actionType", "UNKNOWN"] }
                ]
            }
        }
    },
    { $group: { _id: { sourceIp: "$sourceIp", sig: "$sig" }, cnt: { $sum: 1 } } },
    { $sort: { "_id.sourceIp": 1, cnt: -1, "_id.sig": 1 } },
    { $group: { _id: "$_id.sourceIp", top: { $push: { sig: "$_id.sig", cnt: "$cnt" } } } },
    { $project: { _id: 0, sourceIp: "$_id", top: { $slice: ["$top", 3] } } }
]).toArray();
// results
resultsQ3 = [
    {
        "sourceIp": "10.250.13.188",
        "top": [
            {
                "sig": "receiving",
                "cnt": 918
            },
            {
                "sig": "served",
                "cnt": 172
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.106.50",
        "top": [
            {
                "sig": "receiving",
                "cnt": 864
            },
            {
                "sig": "served",
                "cnt": 172
            },
            {
                "sig": "received",
                "cnt": 2
            }
        ]
    },
    {
        "sourceIp": "10.251.30.179",
        "top": [
            {
                "sig": "receiving",
                "cnt": 847
            },
            {
                "sig": "served",
                "cnt": 166
            }
        ]
    },
    {
        "sourceIp": "10.251.107.98",
        "top": [
            {
                "sig": "receiving",
                "cnt": 738
            },
            {
                "sig": "served",
                "cnt": 171
            },
            {
                "sig": "received",
                "cnt": 2
            }
        ]
    },
    {
        "sourceIp": "10.251.42.16",
        "top": [
            {
                "sig": "receiving",
                "cnt": 842
            },
            {
                "sig": "served",
                "cnt": 187
            },
            {
                "sig": "replicate",
                "cnt": 4
            }
        ]
    },
    {
        "sourceIp": "10.250.6.214",
        "top": [
            {
                "sig": "receiving",
                "cnt": 893
            },
            {
                "sig": "served",
                "cnt": 160
            }
        ]
    },
    {
        "sourceIp": "10.251.109.236",
        "top": [
            {
                "sig": "receiving",
                "cnt": 781
            },
            {
                "sig": "served",
                "cnt": 175
            },
            {
                "sig": "replicate",
                "cnt": 2
            }
        ]
    },
    {
        "sourceIp": "10.251.123.20",
        "top": [
            {
                "sig": "receiving",
                "cnt": 915
            },
            {
                "sig": "served",
                "cnt": 192
            },
            {
                "sig": "received",
                "cnt": 3
            }
        ]
    },
    {
        "sourceIp": "10.251.110.160",
        "top": [
            {
                "sig": "receiving",
                "cnt": 846
            },
            {
                "sig": "served",
                "cnt": 155
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.29.239",
        "top": [
            {
                "sig": "receiving",
                "cnt": 833
            },
            {
                "sig": "served",
                "cnt": 174
            }
        ]
    },
    {
        "sourceIp": "10.251.123.132",
        "top": [
            {
                "sig": "receiving",
                "cnt": 641
            },
            {
                "sig": "served",
                "cnt": 178
            }
        ]
    },
    {
        "sourceIp": "10.250.15.240",
        "top": [
            {
                "sig": "receiving",
                "cnt": 876
            },
            {
                "sig": "served",
                "cnt": 171
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.39.144",
        "top": [
            {
                "sig": "receiving",
                "cnt": 880
            },
            {
                "sig": "served",
                "cnt": 153
            }
        ]
    },
    {
        "sourceIp": "10.251.199.86",
        "top": [
            {
                "sig": "receiving",
                "cnt": 872
            },
            {
                "sig": "served",
                "cnt": 169
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.250.17.225",
        "top": [
            {
                "sig": "receiving",
                "cnt": 874
            },
            {
                "sig": "served",
                "cnt": 163
            }
        ]
    },
    {
        "sourceIp": "10.251.203.129",
        "top": [
            {
                "sig": "receiving",
                "cnt": 618
            },
            {
                "sig": "served",
                "cnt": 186
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.203.166",
        "top": [
            {
                "sig": "receiving",
                "cnt": 803
            },
            {
                "sig": "served",
                "cnt": 162
            }
        ]
    },
    {
        "sourceIp": "10.251.39.160",
        "top": [
            {
                "sig": "receiving",
                "cnt": 336
            },
            {
                "sig": "served",
                "cnt": 104
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.39.64",
        "top": [
            {
                "sig": "receiving",
                "cnt": 902
            },
            {
                "sig": "served",
                "cnt": 178
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.125.193",
        "top": [
            {
                "sig": "receiving",
                "cnt": 901
            },
            {
                "sig": "served",
                "cnt": 164
            }
        ]
    },
    {
        "sourceIp": "10.251.66.102",
        "top": [
            {
                "sig": "receiving",
                "cnt": 907
            },
            {
                "sig": "served",
                "cnt": 173
            },
            {
                "sig": "received",
                "cnt": 3
            }
        ]
    },
    {
        "sourceIp": "10.251.197.226",
        "top": [
            {
                "sig": "receiving",
                "cnt": 849
            },
            {
                "sig": "served",
                "cnt": 251
            }
        ]
    },
    {
        "sourceIp": "10.250.18.114",
        "top": [
            {
                "sig": "receiving",
                "cnt": 901
            },
            {
                "sig": "served",
                "cnt": 166
            },
            {
                "sig": "received",
                "cnt": 3
            }
        ]
    },
    {
        "sourceIp": "10.251.111.228",
        "top": [
            {
                "sig": "receiving",
                "cnt": 849
            },
            {
                "sig": "served",
                "cnt": 167
            }
        ]
    },
    {
        "sourceIp": "10.251.43.21",
        "top": [
            {
                "sig": "receiving",
                "cnt": 909
            },
            {
                "sig": "served",
                "cnt": 155
            },
            {
                "sig": "received",
                "cnt": 3
            }
        ]
    },
    {
        "sourceIp": "10.250.7.230",
        "top": [
            {
                "sig": "receiving",
                "cnt": 825
            },
            {
                "sig": "served",
                "cnt": 208
            },
            {
                "sig": "received",
                "cnt": 3
            }
        ]
    },
    {
        "sourceIp": "10.251.195.52",
        "top": [
            {
                "sig": "receiving",
                "cnt": 902
            },
            {
                "sig": "served",
                "cnt": 172
            },
            {
                "sig": "received",
                "cnt": 2
            }
        ]
    },
    {
        "sourceIp": "10.251.199.159",
        "top": [
            {
                "sig": "receiving",
                "cnt": 883
            },
            {
                "sig": "served",
                "cnt": 178
            }
        ]
    },
    {
        "sourceIp": "10.251.42.84",
        "top": [
            {
                "sig": "receiving",
                "cnt": 863
            },
            {
                "sig": "served",
                "cnt": 182
            },
            {
                "sig": "replicate",
                "cnt": 2
            }
        ]
    },
    {
        "sourceIp": "10.251.43.115",
        "top": [
            {
                "sig": "receiving",
                "cnt": 846
            },
            {
                "sig": "served",
                "cnt": 172
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.43.147",
        "top": [
            {
                "sig": "receiving",
                "cnt": 894
            },
            {
                "sig": "served",
                "cnt": 171
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.30.85",
        "top": [
            {
                "sig": "receiving",
                "cnt": 784
            },
            {
                "sig": "served",
                "cnt": 174
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.67.113",
        "top": [
            {
                "sig": "receiving",
                "cnt": 939
            },
            {
                "sig": "served",
                "cnt": 156
            }
        ]
    },
    {
        "sourceIp": "10.250.7.146",
        "top": [
            {
                "sig": "receiving",
                "cnt": 869
            },
            {
                "sig": "served",
                "cnt": 175
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.214.112",
        "top": [
            {
                "sig": "receiving",
                "cnt": 902
            },
            {
                "sig": "served",
                "cnt": 183
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.250.10.100",
        "top": [
            {
                "sig": "receiving",
                "cnt": 775
            },
            {
                "sig": "served",
                "cnt": 183
            }
        ]
    },
    {
        "sourceIp": "10.251.201.204",
        "top": [
            {
                "sig": "receiving",
                "cnt": 814
            },
            {
                "sig": "served",
                "cnt": 174
            }
        ]
    },
    {
        "sourceIp": "10.251.111.37",
        "top": [
            {
                "sig": "receiving",
                "cnt": 840
            },
            {
                "sig": "served",
                "cnt": 160
            }
        ]
    },
    {
        "sourceIp": "10.251.199.150",
        "top": [
            {
                "sig": "receiving",
                "cnt": 886
            },
            {
                "sig": "served",
                "cnt": 182
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.75.143",
        "top": [
            {
                "sig": "receiving",
                "cnt": 868
            },
            {
                "sig": "served",
                "cnt": 182
            }
        ]
    },
    {
        "sourceIp": "10.251.123.33",
        "top": [
            {
                "sig": "receiving",
                "cnt": 413
            },
            {
                "sig": "served",
                "cnt": 111
            }
        ]
    },
    {
        "sourceIp": "10.251.107.196",
        "top": [
            {
                "sig": "receiving",
                "cnt": 751
            },
            {
                "sig": "served",
                "cnt": 163
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.250.14.224",
        "top": [
            {
                "sig": "receiving",
                "cnt": 670
            },
            {
                "sig": "served",
                "cnt": 197
            },
            {
                "sig": "replicate",
                "cnt": 2
            }
        ]
    },
    {
        "sourceIp": "10.251.110.68",
        "top": [
            {
                "sig": "receiving",
                "cnt": 857
            },
            {
                "sig": "served",
                "cnt": 163
            }
        ]
    },
    {
        "sourceIp": "10.251.42.207",
        "top": [
            {
                "sig": "receiving",
                "cnt": 863
            },
            {
                "sig": "served",
                "cnt": 168
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.70.37",
        "top": [
            {
                "sig": "receiving",
                "cnt": 881
            },
            {
                "sig": "served",
                "cnt": 169
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.193.224",
        "top": [
            {
                "sig": "receiving",
                "cnt": 797
            },
            {
                "sig": "served",
                "cnt": 181
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.90.239",
        "top": [
            {
                "sig": "receiving",
                "cnt": 867
            },
            {
                "sig": "served",
                "cnt": 177
            }
        ]
    },
    {
        "sourceIp": "10.250.6.223",
        "top": [
            {
                "sig": "receiving",
                "cnt": 863
            },
            {
                "sig": "served",
                "cnt": 199
            },
            {
                "sig": "replicate",
                "cnt": 3
            }
        ]
    },
    {
        "sourceIp": "10.250.7.244",
        "top": [
            {
                "sig": "receiving",
                "cnt": 850
            },
            {
                "sig": "served",
                "cnt": 177
            },
            {
                "sig": "received",
                "cnt": 2
            }
        ]
    },
    {
        "sourceIp": "10.251.67.211",
        "top": [
            {
                "sig": "receiving",
                "cnt": 888
            },
            {
                "sig": "served",
                "cnt": 179
            },
            {
                "sig": "replicate",
                "cnt": 3
            }
        ]
    },
    {
        "sourceIp": "10.251.202.209",
        "top": [
            {
                "sig": "receiving",
                "cnt": 930
            },
            {
                "sig": "served",
                "cnt": 171
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.214.225",
        "top": [
            {
                "sig": "receiving",
                "cnt": 735
            },
            {
                "sig": "served",
                "cnt": 153
            }
        ]
    },
    {
        "sourceIp": "10.251.198.196",
        "top": [
            {
                "sig": "receiving",
                "cnt": 644
            },
            {
                "sig": "served",
                "cnt": 175
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.91.159",
        "top": [
            {
                "sig": "receiving",
                "cnt": 851
            },
            {
                "sig": "served",
                "cnt": 174
            },
            {
                "sig": "received",
                "cnt": 2
            }
        ]
    },
    {
        "sourceIp": "10.251.91.229",
        "top": [
            {
                "sig": "receiving",
                "cnt": 902
            },
            {
                "sig": "served",
                "cnt": 181
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.121.224",
        "top": [
            {
                "sig": "receiving",
                "cnt": 872
            },
            {
                "sig": "served",
                "cnt": 171
            }
        ]
    },
    {
        "sourceIp": "10.250.15.67",
        "top": [
            {
                "sig": "receiving",
                "cnt": 791
            },
            {
                "sig": "served",
                "cnt": 184
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.250.11.53",
        "top": [
            {
                "sig": "receiving",
                "cnt": 917
            },
            {
                "sig": "served",
                "cnt": 147
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.215.192",
        "top": [
            {
                "sig": "receiving",
                "cnt": 901
            },
            {
                "sig": "served",
                "cnt": 175
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.27.63",
        "top": [
            {
                "sig": "receiving",
                "cnt": 876
            },
            {
                "sig": "served",
                "cnt": 199
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.30.134",
        "top": [
            {
                "sig": "receiving",
                "cnt": 784
            },
            {
                "sig": "served",
                "cnt": 141
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.214.130",
        "top": [
            {
                "sig": "receiving",
                "cnt": 647
            },
            {
                "sig": "served",
                "cnt": 205
            }
        ]
    },
    {
        "sourceIp": "10.251.107.50",
        "top": [
            {
                "sig": "receiving",
                "cnt": 585
            },
            {
                "sig": "served",
                "cnt": 151
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.31.160",
        "top": [
            {
                "sig": "receiving",
                "cnt": 900
            },
            {
                "sig": "served",
                "cnt": 179
            }
        ]
    },
    {
        "sourceIp": "10.251.35.1",
        "top": [
            {
                "sig": "receiving",
                "cnt": 813
            },
            {
                "sig": "served",
                "cnt": 178
            }
        ]
    },
    {
        "sourceIp": "10.251.39.209",
        "top": [
            {
                "sig": "receiving",
                "cnt": 901
            },
            {
                "sig": "served",
                "cnt": 160
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.111.209",
        "top": [
            {
                "sig": "receiving",
                "cnt": 891
            },
            {
                "sig": "served",
                "cnt": 203
            },
            {
                "sig": "received",
                "cnt": 2
            }
        ]
    },
    {
        "sourceIp": "10.251.70.112",
        "top": [
            {
                "sig": "receiving",
                "cnt": 926
            },
            {
                "sig": "served",
                "cnt": 176
            }
        ]
    },
    {
        "sourceIp": "10.251.126.227",
        "top": [
            {
                "sig": "receiving",
                "cnt": 885
            },
            {
                "sig": "served",
                "cnt": 165
            },
            {
                "sig": "received",
                "cnt": 2
            }
        ]
    },
    {
        "sourceIp": "10.251.31.180",
        "top": [
            {
                "sig": "receiving",
                "cnt": 875
            },
            {
                "sig": "served",
                "cnt": 157
            }
        ]
    },
    {
        "sourceIp": "10.251.71.193",
        "top": [
            {
                "sig": "receiving",
                "cnt": 925
            },
            {
                "sig": "served",
                "cnt": 209
            }
        ]
    },
    {
        "sourceIp": "10.250.10.144",
        "top": [
            {
                "sig": "receiving",
                "cnt": 760
            },
            {
                "sig": "served",
                "cnt": 174
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.38.197",
        "top": [
            {
                "sig": "receiving",
                "cnt": 907
            },
            {
                "sig": "served",
                "cnt": 184
            }
        ]
    },
    {
        "sourceIp": "10.251.122.65",
        "top": [
            {
                "sig": "receiving",
                "cnt": 777
            },
            {
                "sig": "served",
                "cnt": 176
            },
            {
                "sig": "received",
                "cnt": 3
            }
        ]
    },
    {
        "sourceIp": "10.251.66.63",
        "top": [
            {
                "sig": "receiving",
                "cnt": 891
            },
            {
                "sig": "served",
                "cnt": 166
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.126.255",
        "top": [
            {
                "sig": "receiving",
                "cnt": 859
            },
            {
                "sig": "served",
                "cnt": 164
            }
        ]
    },
    {
        "sourceIp": "10.251.110.196",
        "top": [
            {
                "sig": "receiving",
                "cnt": 839
            },
            {
                "sig": "served",
                "cnt": 168
            }
        ]
    },
    {
        "sourceIp": "10.251.126.22",
        "top": [
            {
                "sig": "receiving",
                "cnt": 864
            },
            {
                "sig": "served",
                "cnt": 166
            }
        ]
    },
    {
        "sourceIp": "10.251.38.53",
        "top": [
            {
                "sig": "receiving",
                "cnt": 873
            },
            {
                "sig": "served",
                "cnt": 161
            },
            {
                "sig": "received",
                "cnt": 2
            }
        ]
    },
    {
        "sourceIp": "10.251.110.8",
        "top": [
            {
                "sig": "receiving",
                "cnt": 886
            },
            {
                "sig": "served",
                "cnt": 170
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.125.237",
        "top": [
            {
                "sig": "receiving",
                "cnt": 879
            },
            {
                "sig": "served",
                "cnt": 174
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.71.68",
        "top": [
            {
                "sig": "receiving",
                "cnt": 883
            },
            {
                "sig": "served",
                "cnt": 158
            }
        ]
    },
    {
        "sourceIp": "10.250.19.227",
        "top": [
            {
                "sig": "receiving",
                "cnt": 830
            },
            {
                "sig": "served",
                "cnt": 164
            },
            {
                "sig": "received",
                "cnt": 3
            }
        ]
    },
    {
        "sourceIp": "10.251.73.220",
        "top": [
            {
                "sig": "receiving",
                "cnt": 873
            },
            {
                "sig": "served",
                "cnt": 176
            }
        ]
    },
    {
        "sourceIp": "10.251.106.10",
        "top": [
            {
                "sig": "receiving",
                "cnt": 575
            },
            {
                "sig": "served",
                "cnt": 138
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.89.155",
        "top": [
            {
                "sig": "receiving",
                "cnt": 871
            },
            {
                "sig": "served",
                "cnt": 160
            },
            {
                "sig": "received",
                "cnt": 2
            }
        ]
    },
    {
        "sourceIp": "10.251.25.237",
        "top": [
            {
                "sig": "receiving",
                "cnt": 838
            },
            {
                "sig": "served",
                "cnt": 187
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.106.214",
        "top": [
            {
                "sig": "receiving",
                "cnt": 742
            },
            {
                "sig": "served",
                "cnt": 182
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.250.19.16",
        "top": [
            {
                "sig": "receiving",
                "cnt": 892
            },
            {
                "sig": "served",
                "cnt": 165
            },
            {
                "sig": "received",
                "cnt": 2
            }
        ]
    },
    {
        "sourceIp": "10.251.105.189",
        "top": [
            {
                "sig": "receiving",
                "cnt": 904
            },
            {
                "sig": "served",
                "cnt": 171
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.73.188",
        "top": [
            {
                "sig": "receiving",
                "cnt": 921
            },
            {
                "sig": "served",
                "cnt": 168
            }
        ]
    },
    {
        "sourceIp": "10.251.42.191",
        "top": [
            {
                "sig": "receiving",
                "cnt": 883
            },
            {
                "sig": "served",
                "cnt": 182
            }
        ]
    },
    {
        "sourceIp": "10.251.127.47",
        "top": [
            {
                "sig": "receiving",
                "cnt": 796
            },
            {
                "sig": "served",
                "cnt": 199
            },
            {
                "sig": "received",
                "cnt": 3
            }
        ]
    },
    {
        "sourceIp": "10.251.194.129",
        "top": [
            {
                "sig": "receiving",
                "cnt": 900
            },
            {
                "sig": "served",
                "cnt": 156
            }
        ]
    },
    {
        "sourceIp": "10.250.11.85",
        "top": [
            {
                "sig": "receiving",
                "cnt": 847
            },
            {
                "sig": "served",
                "cnt": 168
            },
            {
                "sig": "replicate",
                "cnt": 3
            }
        ]
    },
    {
        "sourceIp": "10.251.195.70",
        "top": [
            {
                "sig": "receiving",
                "cnt": 893
            },
            {
                "sig": "served",
                "cnt": 164
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.31.5",
        "top": [
            {
                "sig": "receiving",
                "cnt": 879
            },
            {
                "sig": "served",
                "cnt": 183
            },
            {
                "sig": "received",
                "cnt": 2
            }
        ]
    },
    {
        "sourceIp": "10.250.14.143",
        "top": [
            {
                "sig": "receiving",
                "cnt": 785
            },
            {
                "sig": "served",
                "cnt": 187
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.250.17.177",
        "top": [
            {
                "sig": "receiving",
                "cnt": 909
            },
            {
                "sig": "served",
                "cnt": 198
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.39.242",
        "top": [
            {
                "sig": "receiving",
                "cnt": 880
            },
            {
                "sig": "served",
                "cnt": 162
            },
            {
                "sig": "replicate",
                "cnt": 2
            }
        ]
    },
    {
        "sourceIp": "10.251.90.134",
        "top": [
            {
                "sig": "receiving",
                "cnt": 935
            },
            {
                "sig": "served",
                "cnt": 150
            }
        ]
    },
    {
        "sourceIp": "10.251.111.130",
        "top": [
            {
                "sig": "receiving",
                "cnt": 868
            },
            {
                "sig": "served",
                "cnt": 172
            }
        ]
    },
    {
        "sourceIp": "10.251.109.209",
        "top": [
            {
                "sig": "receiving",
                "cnt": 619
            },
            {
                "sig": "served",
                "cnt": 195
            },
            {
                "sig": "received",
                "cnt": 2
            }
        ]
    },
    {
        "sourceIp": "10.250.5.161",
        "top": [
            {
                "sig": "receiving",
                "cnt": 788
            },
            {
                "sig": "served",
                "cnt": 171
            }
        ]
    },
    {
        "sourceIp": "10.251.123.195",
        "top": [
            {
                "sig": "receiving",
                "cnt": 888
            },
            {
                "sig": "served",
                "cnt": 160
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.194.102",
        "top": [
            {
                "sig": "receiving",
                "cnt": 846
            },
            {
                "sig": "served",
                "cnt": 167
            }
        ]
    },
    {
        "sourceIp": "10.251.67.225",
        "top": [
            {
                "sig": "receiving",
                "cnt": 912
            },
            {
                "sig": "served",
                "cnt": 157
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.107.19",
        "top": [
            {
                "sig": "receiving",
                "cnt": 902
            },
            {
                "sig": "served",
                "cnt": 202
            },
            {
                "sig": "replicate",
                "cnt": 2
            }
        ]
    },
    {
        "sourceIp": "10.251.106.37",
        "top": [
            {
                "sig": "receiving",
                "cnt": 912
            },
            {
                "sig": "served",
                "cnt": 182
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.250.10.223",
        "top": [
            {
                "sig": "receiving",
                "cnt": 854
            },
            {
                "sig": "served",
                "cnt": 168
            },
            {
                "sig": "received",
                "cnt": 2
            }
        ]
    },
    {
        "sourceIp": "10.251.194.213",
        "top": [
            {
                "sig": "receiving",
                "cnt": 809
            },
            {
                "sig": "served",
                "cnt": 243
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.203.149",
        "top": [
            {
                "sig": "receiving",
                "cnt": 627
            },
            {
                "sig": "served",
                "cnt": 198
            }
        ]
    },
    {
        "sourceIp": "10.251.199.245",
        "top": [
            {
                "sig": "receiving",
                "cnt": 864
            },
            {
                "sig": "served",
                "cnt": 171
            }
        ]
    },
    {
        "sourceIp": "10.251.37.240",
        "top": [
            {
                "sig": "receiving",
                "cnt": 841
            },
            {
                "sig": "served",
                "cnt": 168
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.67.4",
        "top": [
            {
                "sig": "receiving",
                "cnt": 816
            },
            {
                "sig": "served",
                "cnt": 177
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.42.9",
        "top": [
            {
                "sig": "receiving",
                "cnt": 637
            },
            {
                "sig": "served",
                "cnt": 180
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.31.85",
        "top": [
            {
                "sig": "receiving",
                "cnt": 884
            },
            {
                "sig": "served",
                "cnt": 182
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.107.227",
        "top": [
            {
                "sig": "receiving",
                "cnt": 769
            },
            {
                "sig": "served",
                "cnt": 157
            }
        ]
    },
    {
        "sourceIp": "10.251.194.245",
        "top": [
            {
                "sig": "receiving",
                "cnt": 819
            },
            {
                "sig": "served",
                "cnt": 194
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.215.70",
        "top": [
            {
                "sig": "receiving",
                "cnt": 780
            },
            {
                "sig": "served",
                "cnt": 173
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.250.10.213",
        "top": [
            {
                "sig": "receiving",
                "cnt": 881
            },
            {
                "sig": "served",
                "cnt": 164
            },
            {
                "sig": "received",
                "cnt": 3
            }
        ]
    },
    {
        "sourceIp": "10.251.194.147",
        "top": [
            {
                "sig": "receiving",
                "cnt": 699
            },
            {
                "sig": "served",
                "cnt": 188
            },
            {
                "sig": "replicate",
                "cnt": 2
            }
        ]
    },
    {
        "sourceIp": "10.251.197.161",
        "top": [
            {
                "sig": "receiving",
                "cnt": 891
            },
            {
                "sig": "served",
                "cnt": 163
            },
            {
                "sig": "received",
                "cnt": 2
            }
        ]
    },
    {
        "sourceIp": "10.251.39.192",
        "top": [
            {
                "sig": "receiving",
                "cnt": 814
            },
            {
                "sig": "served",
                "cnt": 202
            }
        ]
    },
    {
        "sourceIp": "10.251.127.191",
        "top": [
            {
                "sig": "receiving",
                "cnt": 871
            },
            {
                "sig": "served",
                "cnt": 186
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.250.9.207",
        "top": [
            {
                "sig": "receiving",
                "cnt": 763
            },
            {
                "sig": "served",
                "cnt": 188
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.203.246",
        "top": [
            {
                "sig": "receiving",
                "cnt": 602
            },
            {
                "sig": "served",
                "cnt": 174
            }
        ]
    },
    {
        "sourceIp": "10.251.71.146",
        "top": [
            {
                "sig": "receiving",
                "cnt": 825
            },
            {
                "sig": "served",
                "cnt": 174
            },
            {
                "sig": "replicate",
                "cnt": 2
            }
        ]
    },
    {
        "sourceIp": "10.251.126.5",
        "top": [
            {
                "sig": "receiving",
                "cnt": 889
            },
            {
                "sig": "served",
                "cnt": 174
            },
            {
                "sig": "received",
                "cnt": 2
            }
        ]
    },
    {
        "sourceIp": "10.251.91.84",
        "top": [
            {
                "sig": "receiving",
                "cnt": 875
            },
            {
                "sig": "served",
                "cnt": 152
            }
        ]
    },
    {
        "sourceIp": "10.251.198.33",
        "top": [
            {
                "sig": "receiving",
                "cnt": 912
            },
            {
                "sig": "served",
                "cnt": 176
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.42.246",
        "top": [
            {
                "sig": "receiving",
                "cnt": 904
            },
            {
                "sig": "served",
                "cnt": 166
            }
        ]
    },
    {
        "sourceIp": "10.251.65.203",
        "top": [
            {
                "sig": "receiving",
                "cnt": 890
            },
            {
                "sig": "served",
                "cnt": 171
            },
            {
                "sig": "received",
                "cnt": 4
            }
        ]
    },
    {
        "sourceIp": "10.251.91.32",
        "top": [
            {
                "sig": "receiving",
                "cnt": 640
            },
            {
                "sig": "served",
                "cnt": 171
            },
            {
                "sig": "received",
                "cnt": 2
            }
        ]
    },
    {
        "sourceIp": "10.251.214.32",
        "top": [
            {
                "sig": "receiving",
                "cnt": 645
            },
            {
                "sig": "served",
                "cnt": 180
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.71.97",
        "top": [
            {
                "sig": "receiving",
                "cnt": 860
            },
            {
                "sig": "served",
                "cnt": 169
            }
        ]
    },
    {
        "sourceIp": "10.251.90.64",
        "top": [
            {
                "sig": "receiving",
                "cnt": 814
            },
            {
                "sig": "served",
                "cnt": 186
            },
            {
                "sig": "received",
                "cnt": 2
            }
        ]
    },
    {
        "sourceIp": "10.251.75.49",
        "top": [
            {
                "sig": "receiving",
                "cnt": 926
            },
            {
                "sig": "served",
                "cnt": 240
            }
        ]
    },
    {
        "sourceIp": "10.250.5.237",
        "top": [
            {
                "sig": "receiving",
                "cnt": 863
            },
            {
                "sig": "served",
                "cnt": 178
            }
        ]
    },
    {
        "sourceIp": "10.251.70.211",
        "top": [
            {
                "sig": "receiving",
                "cnt": 870
            },
            {
                "sig": "served",
                "cnt": 175
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.31.242",
        "top": [
            {
                "sig": "receiving",
                "cnt": 917
            },
            {
                "sig": "served",
                "cnt": 178
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.43.210",
        "top": [
            {
                "sig": "receiving",
                "cnt": 926
            },
            {
                "sig": "served",
                "cnt": 160
            }
        ]
    },
    {
        "sourceIp": "10.251.71.240",
        "top": [
            {
                "sig": "receiving",
                "cnt": 873
            },
            {
                "sig": "served",
                "cnt": 199
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.65.237",
        "top": [
            {
                "sig": "receiving",
                "cnt": 931
            },
            {
                "sig": "served",
                "cnt": 165
            },
            {
                "sig": "received",
                "cnt": 2
            }
        ]
    },
    {
        "sourceIp": "10.251.203.80",
        "top": [
            {
                "sig": "receiving",
                "cnt": 830
            },
            {
                "sig": "served",
                "cnt": 167
            },
            {
                "sig": "replicate",
                "cnt": 2
            }
        ]
    },
    {
        "sourceIp": "10.251.70.5",
        "top": [
            {
                "sig": "receiving",
                "cnt": 887
            },
            {
                "sig": "served",
                "cnt": 182
            }
        ]
    },
    {
        "sourceIp": "10.251.90.81",
        "top": [
            {
                "sig": "receiving",
                "cnt": 854
            },
            {
                "sig": "served",
                "cnt": 167
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.43.192",
        "top": [
            {
                "sig": "receiving",
                "cnt": 894
            },
            {
                "sig": "served",
                "cnt": 157
            }
        ]
    },
    {
        "sourceIp": "10.251.111.80",
        "top": [
            {
                "sig": "receiving",
                "cnt": 891
            },
            {
                "sig": "served",
                "cnt": 160
            }
        ]
    },
    {
        "sourceIp": "10.251.215.16",
        "top": [
            {
                "sig": "receiving",
                "cnt": 832
            },
            {
                "sig": "served",
                "cnt": 182
            },
            {
                "sig": "received",
                "cnt": 2
            }
        ]
    },
    {
        "sourceIp": "10.250.11.194",
        "top": [
            {
                "sig": "receiving",
                "cnt": 882
            },
            {
                "sig": "served",
                "cnt": 186
            },
            {
                "sig": "received",
                "cnt": 2
            }
        ]
    },
    {
        "sourceIp": "10.251.75.163",
        "top": [
            {
                "sig": "receiving",
                "cnt": 797
            },
            {
                "sig": "served",
                "cnt": 160
            }
        ]
    },
    {
        "sourceIp": "10.251.203.179",
        "top": [
            {
                "sig": "receiving",
                "cnt": 774
            },
            {
                "sig": "served",
                "cnt": 166
            },
            {
                "sig": "received",
                "cnt": 2
            }
        ]
    },
    {
        "sourceIp": "10.251.71.16",
        "top": [
            {
                "sig": "receiving",
                "cnt": 864
            },
            {
                "sig": "served",
                "cnt": 161
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.199.225",
        "top": [
            {
                "sig": "receiving",
                "cnt": 818
            },
            {
                "sig": "served",
                "cnt": 165
            },
            {
                "sig": "received",
                "cnt": 2
            }
        ]
    },
    {
        "sourceIp": "10.250.10.6",
        "top": [
            {
                "sig": "receiving",
                "cnt": 879
            },
            {
                "sig": "served",
                "cnt": 207
            }
        ]
    },
    {
        "sourceIp": "10.251.38.214",
        "top": [
            {
                "sig": "receiving",
                "cnt": 858
            },
            {
                "sig": "served",
                "cnt": 170
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.250.13.240",
        "top": [
            {
                "sig": "receiving",
                "cnt": 845
            },
            {
                "sig": "served",
                "cnt": 162
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.122.79",
        "top": [
            {
                "sig": "receiving",
                "cnt": 749
            },
            {
                "sig": "served",
                "cnt": 171
            }
        ]
    },
    {
        "sourceIp": "10.251.199.19",
        "top": [
            {
                "sig": "receiving",
                "cnt": 857
            },
            {
                "sig": "served",
                "cnt": 162
            }
        ]
    },
    {
        "sourceIp": "10.251.202.181",
        "top": [
            {
                "sig": "receiving",
                "cnt": 760
            },
            {
                "sig": "served",
                "cnt": 162
            }
        ]
    },
    {
        "sourceIp": "10.251.74.227",
        "top": [
            {
                "sig": "receiving",
                "cnt": 860
            },
            {
                "sig": "served",
                "cnt": 183
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.215.50",
        "top": [
            {
                "sig": "receiving",
                "cnt": 891
            },
            {
                "sig": "served",
                "cnt": 170
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.250.11.100",
        "top": [
            {
                "sig": "receiving",
                "cnt": 874
            },
            {
                "sig": "served",
                "cnt": 259
            }
        ]
    },
    {
        "sourceIp": "10.251.195.33",
        "top": [
            {
                "sig": "receiving",
                "cnt": 846
            },
            {
                "sig": "served",
                "cnt": 164
            }
        ]
    },
    {
        "sourceIp": "10.250.14.196",
        "top": [
            {
                "sig": "receiving",
                "cnt": 915
            },
            {
                "sig": "served",
                "cnt": 169
            },
            {
                "sig": "received",
                "cnt": 2
            }
        ]
    },
    {
        "sourceIp": "10.251.193.175",
        "top": [
            {
                "sig": "receiving",
                "cnt": 815
            },
            {
                "sig": "served",
                "cnt": 159
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.26.8",
        "top": [
            {
                "sig": "receiving",
                "cnt": 787
            },
            {
                "sig": "served",
                "cnt": 162
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.39.179",
        "top": [
            {
                "sig": "receiving",
                "cnt": 921
            },
            {
                "sig": "served",
                "cnt": 222
            },
            {
                "sig": "received",
                "cnt": 3
            }
        ]
    },
    {
        "sourceIp": "10.251.123.99",
        "top": [
            {
                "sig": "receiving",
                "cnt": 909
            },
            {
                "sig": "served",
                "cnt": 185
            }
        ]
    },
    {
        "sourceIp": "10.251.74.79",
        "top": [
            {
                "sig": "receiving",
                "cnt": 935
            },
            {
                "sig": "served",
                "cnt": 196
            },
            {
                "sig": "received",
                "cnt": 2
            }
        ]
    },
    {
        "sourceIp": "10.251.126.83",
        "top": [
            {
                "sig": "receiving",
                "cnt": 871
            },
            {
                "sig": "served",
                "cnt": 177
            }
        ]
    },
    {
        "sourceIp": "10.251.30.101",
        "top": [
            {
                "sig": "receiving",
                "cnt": 919
            },
            {
                "sig": "served",
                "cnt": 169
            }
        ]
    },
    {
        "sourceIp": "10.251.123.1",
        "top": [
            {
                "sig": "receiving",
                "cnt": 813
            },
            {
                "sig": "served",
                "cnt": 177
            }
        ]
    },
    {
        "sourceIp": "10.251.125.174",
        "top": [
            {
                "sig": "receiving",
                "cnt": 919
            },
            {
                "sig": "served",
                "cnt": 164
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.214.175",
        "top": [
            {
                "sig": "receiving",
                "cnt": 889
            },
            {
                "sig": "served",
                "cnt": 167
            },
            {
                "sig": "received",
                "cnt": 2
            }
        ]
    },
    {
        "sourceIp": "10.251.30.6",
        "top": [
            {
                "sig": "receiving",
                "cnt": 766
            },
            {
                "sig": "served",
                "cnt": 180
            },
            {
                "sig": "received",
                "cnt": 3
            }
        ]
    },
    {
        "sourceIp": "10.251.66.3",
        "top": [
            {
                "sig": "receiving",
                "cnt": 848
            },
            {
                "sig": "served",
                "cnt": 190
            }
        ]
    },
    {
        "sourceIp": "10.250.19.102",
        "top": [
            {
                "sig": "receiving",
                "cnt": 10
            }
        ]
    },
    {
        "sourceIp": "10.250.7.96",
        "top": [
            {
                "sig": "receiving",
                "cnt": 909
            },
            {
                "sig": "served",
                "cnt": 161
            }
        ]
    },
    {
        "sourceIp": "10.250.7.32",
        "top": [
            {
                "sig": "receiving",
                "cnt": 875
            },
            {
                "sig": "served",
                "cnt": 182
            },
            {
                "sig": "replicate",
                "cnt": 3
            }
        ]
    },
    {
        "sourceIp": "10.251.26.131",
        "top": [
            {
                "sig": "receiving",
                "cnt": 880
            },
            {
                "sig": "served",
                "cnt": 151
            }
        ]
    },
    {
        "sourceIp": "10.251.26.177",
        "top": [
            {
                "sig": "receiving",
                "cnt": 857
            },
            {
                "sig": "served",
                "cnt": 157
            }
        ]
    },
    {
        "sourceIp": "10.251.74.134",
        "top": [
            {
                "sig": "receiving",
                "cnt": 946
            },
            {
                "sig": "served",
                "cnt": 167
            },
            {
                "sig": "received",
                "cnt": 2
            }
        ]
    },
    {
        "sourceIp": "10.251.214.67",
        "top": [
            {
                "sig": "receiving",
                "cnt": 870
            },
            {
                "sig": "served",
                "cnt": 187
            }
        ]
    },
    {
        "sourceIp": "10.250.15.198",
        "top": [
            {
                "sig": "receiving",
                "cnt": 862
            },
            {
                "sig": "served",
                "cnt": 169
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.74.192",
        "top": [
            {
                "sig": "receiving",
                "cnt": 925
            },
            {
                "sig": "served",
                "cnt": 185
            }
        ]
    },
    {
        "sourceIp": "10.251.214.18",
        "top": [
            {
                "sig": "receiving",
                "cnt": 751
            },
            {
                "sig": "served",
                "cnt": 165
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.75.79",
        "top": [
            {
                "sig": "receiving",
                "cnt": 854
            },
            {
                "sig": "served",
                "cnt": 193
            }
        ]
    },
    {
        "sourceIp": "10.250.14.38",
        "top": [
            {
                "sig": "receiving",
                "cnt": 874
            },
            {
                "sig": "served",
                "cnt": 158
            }
        ]
    },
    {
        "sourceIp": "10.251.75.228",
        "top": [
            {
                "sig": "receiving",
                "cnt": 863
            },
            {
                "sig": "served",
                "cnt": 159
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.202.134",
        "top": [
            {
                "sig": "receiving",
                "cnt": 643
            },
            {
                "sig": "served",
                "cnt": 173
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.107.242",
        "top": [
            {
                "sig": "receiving",
                "cnt": 876
            },
            {
                "sig": "served",
                "cnt": 144
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.250.6.191",
        "top": [
            {
                "sig": "receiving",
                "cnt": 867
            },
            {
                "sig": "served",
                "cnt": 185
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.91.15",
        "top": [
            {
                "sig": "receiving",
                "cnt": 901
            },
            {
                "sig": "served",
                "cnt": 183
            }
        ]
    },
    {
        "sourceIp": "10.251.66.192",
        "top": [
            {
                "sig": "receiving",
                "cnt": 903
            },
            {
                "sig": "served",
                "cnt": 232
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.122.38",
        "top": [
            {
                "sig": "receiving",
                "cnt": 897
            },
            {
                "sig": "served",
                "cnt": 192
            }
        ]
    },
    {
        "sourceIp": "10.250.6.4",
        "top": [
            {
                "sig": "receiving",
                "cnt": 821
            },
            {
                "sig": "served",
                "cnt": 172
            },
            {
                "sig": "received",
                "cnt": 2
            }
        ]
    },
    {
        "sourceIp": "10.250.15.101",
        "top": [
            {
                "sig": "receiving",
                "cnt": 887
            },
            {
                "sig": "served",
                "cnt": 171
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.250.10.176",
        "top": [
            {
                "sig": "receiving",
                "cnt": 867
            },
            {
                "sig": "served",
                "cnt": 174
            }
        ]
    },
    {
        "sourceIp": "10.251.127.243",
        "top": [
            {
                "sig": "receiving",
                "cnt": 668
            },
            {
                "sig": "served",
                "cnt": 182
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    },
    {
        "sourceIp": "10.251.203.4",
        "top": [
            {
                "sig": "receiving",
                "cnt": 814
            },
            {
                "sig": "served",
                "cnt": 156
            },
            {
                "sig": "received",
                "cnt": 1
            }
        ]
    }
];

/* ============================================================
   Q4. Two least common HTTP methods in a time range
   Time range: 2005-06-09 .. 2006-02-28 (UTC)
============================================================ */
print("\nQ4: Two least common HTTP methods (2005-06-09..2006-02-28)");
db.logs.aggregate([
    { $match: { logSet: "ACCESS", ts: { $gte: ISODate("2005-06-09T00:00:00Z"), $lte: ISODate("2006-02-28T23:59:59Z") } } },
    { $group: { _id: "$access.method", cnt: { $sum: 1 } } },
    { $sort: { cnt: 1 } },
    { $limit: 2 }
]).toArray();
// results
resultsQ4 = [
    {
        "_id": "OPTIONS",
        "cnt": 7
    },
    {
        "_id": "PUT",
        "cnt": 20
    }
];


/* ============================================================
   Q5. Top 5 source IPs with most 4xx errors
   Time range: 2005-06-09 .. 2006-02-28 (UTC)
============================================================ */
print("\nQ5: Top 5 source IPs with most 4xx errors (2005-06-09..2006-02-28)");
db.logs.aggregate([
    { $match: { logSet: "ACCESS", ts: { $gte: ISODate("2005-06-09T00:00:00Z"), $lte: ISODate("2006-02-28T23:59:59Z") } } },
    { $match: { "access.status": { $gte: 400, $lt: 500 } } },
    { $group: { _id: "$access.sourceIp", cnt: { $sum: 1 } } },
    { $sort: { cnt: -1 } },
    { $limit: 5 }
]).toArray();
// results
resultsQ5 = [
    {
        "_id": "10.251.122.38",
        "cnt": 11
    },
    {
        "_id": "10.251.202.134",
        "cnt": 10
    },
    {
        "_id": "10.251.107.242",
        "cnt": 10
    },
    {
        "_id": "10.251.214.18",
        "cnt": 9
    },
    {
        "_id": "10.251.75.79",
        "cnt": 9
    }
];


/* ============================================================
   Q6. Top 10 URLs requested by source IP 10.251.122.38
   Time range: 2005-06-09 .. 2006-02-28 (UTC)
============================================================ */
print("\nQ6: Top 10 URLs requested by 10.251.122.38 (2005-06-09..2006-02-28)");
db.logs.aggregate([
    { $match: { logSet: "ACCESS", ts: { $gte: ISODate("2005-06-09T00:00:00Z"), $lte: ISODate("2006-02-28T23:59:59Z") } } },
    { $match: { "access.sourceIp": "10.251.122.38" } },
    { $group: { _id: "$access.url", cnt: { $sum: 1 } } },
    { $sort: { cnt: -1 } },
    { $limit: 10 }
]).toArray();
// results
resultsQ6 = [
    {
        "_id": "/",
        "cnt": 100
    },
    {
        "_id": "/index.html",
        "cnt": 100
    },
    {
        "_id": "/images/logo.gif",
        "cnt": 100
    },
    {
        "_id": "/images/bg.gif",
        "cnt": 100
    },
    {
        "_id": "/images/bg_top.gif",
        "cnt": 100
    },
    {
        "_id": "/images/bg_bottom.gif",
        "cnt": 100
    },
    {
        "_id": "/images/bg_left.gif",
        "cnt": 100
    },
    {
        "_id": "/images/bg_right.gif",
        "cnt": 100
    },
    {
        "_id": "/images/bg_topleft.gif",
        "cnt": 100
    },
    {
        "_id": "/images/bg_topright.gif",
        "cnt": 100
    }
];



/* ============================================================
   Q7. Fifty most upvoted logs for a day
   Day: 2008-11-09
============================================================ */
print("\nQ7: Top 50 upvoted logs (day=2008-11-09)");
db.logs.find({ day: "2008-11-09" }).sort({ upvoteCount: -1 }).limit(50).toArray();
// results
resultsQ7 = [
    {
        "_id": {
            "$oid": "69743a07333a5ad8dee58cf2"
        },
        "logSet": "HDFS_DATAXCEIVER",
        "actionType": "receiving",
        "ts": {
            "$date": "2008-11-09T20:45:08Z"
        },
        "day": "2008-11-09",
        "sourceIp": "10.251.110.68",
        "destIp": "10.251.110.68",
        "blockId": -8462265999861166000,
        "sizeBytes": null,
        "upvoteCount": 9,
        "ingestKey": "9fbdee4325cc6841058fbb52d0567ab88183bb0f787693a5a88e9070dbb204cf"
    },
    {
        "_id": {
            "$oid": "69743a0772096bfb9be5a675"
        },
        "logSet": "HDFS_NAMESYSTEM",
        "ingestKey": "78a9c1bb05c471e11e93a3ac3e699de16e9a4fe8108ed99674893d4c94e152fb",
        "actionType": "update",
        "ts": {
            "$date": "2008-11-09T20:49:28Z"
        },
        "day": "2008-11-09",
        "sourceIp": null,
        "destIp": "10.251.126.22",
        "blockId": 7482385859321917000,
        "sizeBytes": 67108864,
        "upvoteCount": 9
    },
    {
        "_id": {
            "$oid": "69743a0772096bfb9be5c137"
        },
        "logSet": "HDFS_NAMESYSTEM",
        "ingestKey": "b0f58915f45b8d9e96857a5e7da3d15d0d8cfd58896beb9af77ec579c2359e1e",
        "actionType": "update",
        "ts": {
            "$date": "2008-11-09T20:53:34Z"
        },
        "day": "2008-11-09",
        "sourceIp": null,
        "destIp": "10.251.43.147",
        "blockId": -6189931824872523000,
        "sizeBytes": 67108864,
        "upvoteCount": 9
    },
    {
        "_id": {
            "$oid": "69743a07333a5ad8dee5d8f5"
        },
        "logSet": "HDFS_DATAXCEIVER",
        "actionType": "receiving",
        "ts": {
            "$date": "2008-11-09T20:56:00Z"
        },
        "day": "2008-11-09",
        "sourceIp": "10.251.109.236",
        "destIp": "10.251.109.236",
        "blockId": -3389646900480585700,
        "sizeBytes": null,
        "upvoteCount": 8,
        "ingestKey": "fb259217e7b0076e2a8254abb47c22475c2550691badbf076ce172c627661e04"
    },
    {
        "_id": {
            "$oid": "69743a0872096bfb9be5f4c4"
        },
        "logSet": "HDFS_NAMESYSTEM",
        "ingestKey": "0d0f0a2f59bfc8385b3542b5881c72ac6cc69e16cde6b02853e9b19b5734a4db",
        "actionType": "update",
        "ts": {
            "$date": "2008-11-09T21:00:49Z"
        },
        "day": "2008-11-09",
        "sourceIp": null,
        "destIp": "10.251.193.175",
        "blockId": 218931434370499400,
        "sizeBytes": 67108864,
        "upvoteCount": 8
    },
    {
        "_id": {
            "$oid": "69743a0872096bfb9be61f95"
        },
        "logSet": "HDFS_NAMESYSTEM",
        "ingestKey": "ae55de3b7fde73190431551d37020159368617c668e284baf0f2cf35107d89ea",
        "actionType": "update",
        "ts": {
            "$date": "2008-11-09T21:07:13Z"
        },
        "day": "2008-11-09",
        "sourceIp": null,
        "destIp": "10.251.75.79",
        "blockId": 7177893307679599000,
        "sizeBytes": 67108864,
        "upvoteCount": 8
    },
    {
        "_id": {
            "$oid": "69743a0872096bfb9be6216b"
        },
        "logSet": "HDFS_NAMESYSTEM",
        "ingestKey": "299998e679ba88925ffb6bd362829e0364a95baa9d8af467644b3fd532e2fd54",
        "actionType": "update",
        "ts": {
            "$date": "2008-11-09T21:07:29Z"
        },
        "day": "2008-11-09",
        "sourceIp": null,
        "destIp": "10.251.75.228",
        "blockId": -1557997489702327000,
        "sizeBytes": 67108864,
        "upvoteCount": 8
    },
    {
        "_id": {
            "$oid": "69743a0872096bfb9be66a2e"
        },
        "logSet": "HDFS_NAMESYSTEM",
        "ingestKey": "4340608dd9efb428c2b01a535af11a51ca217c1ed08632dd75362d5c139b2618",
        "actionType": "update",
        "ts": {
            "$date": "2008-11-09T21:17:57Z"
        },
        "day": "2008-11-09",
        "sourceIp": null,
        "destIp": "10.251.71.193",
        "blockId": -3808739046859638000,
        "sizeBytes": 67108864,
        "upvoteCount": 8
    },
    {
        "_id": {
            "$oid": "69743a0872096bfb9be66ab2"
        },
        "logSet": "HDFS_NAMESYSTEM",
        "ingestKey": "a4731b0cf4f32a1d44a3bd347513ebe7b6fa465e173cd41a79e9bb846f5e0bcf",
        "actionType": "update",
        "ts": {
            "$date": "2008-11-09T21:18:02Z"
        },
        "day": "2008-11-09",
        "sourceIp": null,
        "destIp": "10.251.122.79",
        "blockId": 331591943750882500,
        "sizeBytes": 67108864,
        "upvoteCount": 8
    },
    {
        "_id": {
            "$oid": "69743a0672096bfb9be54a0d"
        },
        "logSet": "HDFS_NAMESYSTEM",
        "ingestKey": "1aeb4a5ee00e494e12473e4b79c48d56d1fb0b3dd0f01034acdfe8241f47e013",
        "actionType": "update",
        "ts": {
            "$date": "2008-11-09T20:36:07Z"
        },
        "day": "2008-11-09",
        "sourceIp": null,
        "destIp": "10.251.38.214",
        "blockId": -1076549517733373600,
        "sizeBytes": 67108864,
        "upvoteCount": 7
    },
    {
        "_id": {
            "$oid": "69743a0672096bfb9be5500a"
        },
        "logSet": "HDFS_NAMESYSTEM",
        "ingestKey": "331257284f72944ac6e620358db09eee2e5f8e58eb7bd2c2df7e780e14335346",
        "actionType": "update",
        "ts": {
            "$date": "2008-11-09T20:37:00Z"
        },
        "day": "2008-11-09",
        "sourceIp": null,
        "destIp": "10.251.123.20",
        "blockId": 3355583568782545400,
        "sizeBytes": 67108864,
        "upvoteCount": 7
    },
    {
        "_id": {
            "$oid": "69743a0672096bfb9be553a1"
        },
        "logSet": "HDFS_NAMESYSTEM",
        "ingestKey": "df3fa4201fdee382ef71b74e0e75eb1aa7c84ed27f99e73077dc4a1ff1047bd5",
        "actionType": "update",
        "ts": {
            "$date": "2008-11-09T20:37:31Z"
        },
        "day": "2008-11-09",
        "sourceIp": null,
        "destIp": "10.251.203.80",
        "blockId": 5167296799288638000,
        "sizeBytes": 67108864,
        "upvoteCount": 7
    },
    {
        "_id": {
            "$oid": "69743a06333a5ad8dee54ce0"
        },
        "logSet": "HDFS_DATAXCEIVER",
        "actionType": "served",
        "ts": {
            "$date": "2008-11-09T20:35:35Z"
        },
        "day": "2008-11-09",
        "sourceIp": "10.251.197.226",
        "destIp": "10.251.26.8",
        "blockId": -3544583377289625600,
        "sizeBytes": null,
        "upvoteCount": 7,
        "ingestKey": "a5577e52cf08742edd0c0c66bda0f0944040ebc121653820f74fab7e6506cda8"
    },
    {
        "_id": {
            "$oid": "69743a06333a5ad8dee551b9"
        },
        "logSet": "HDFS_DATAXCEIVER",
        "actionType": "receiving",
        "ts": {
            "$date": "2008-11-09T20:36:20Z"
        },
        "day": "2008-11-09",
        "sourceIp": "10.251.67.113",
        "destIp": "10.251.67.113",
        "blockId": -1671908550880445700,
        "sizeBytes": null,
        "upvoteCount": 7,
        "ingestKey": "b853e50e39af8156ec6e4fdca8d72ae0e4e6dae74fb65b0d56c2ed0ff7d10c6e"
    },
    {
        "_id": {
            "$oid": "69743a06333a5ad8dee55458"
        },
        "logSet": "HDFS_DATAXCEIVER",
        "actionType": "receiving",
        "ts": {
            "$date": "2008-11-09T20:36:42Z"
        },
        "day": "2008-11-09",
        "sourceIp": "10.251.31.160",
        "destIp": "10.251.31.160",
        "blockId": -7915807112303457000,
        "sizeBytes": null,
        "upvoteCount": 7,
        "ingestKey": "42d9c89ca3c1bee88b24b1b526a7ea7b295c469a2daadc93358e8a74bb8a4865"
    },
    {
        "_id": {
            "$oid": "69743a06333a5ad8dee55588"
        },
        "logSet": "HDFS_DATAXCEIVER",
        "actionType": "receiving",
        "ts": {
            "$date": "2008-11-09T20:36:54Z"
        },
        "day": "2008-11-09",
        "sourceIp": "10.251.27.63",
        "destIp": "10.251.27.63",
        "blockId": -2590057905078537000,
        "sizeBytes": null,
        "upvoteCount": 7,
        "ingestKey": "2d15c38e158032c28714998319f58f24b672f3f76880d91a505e7428e7249f6b"
    },
    {
        "_id": {
            "$oid": "69743a06333a5ad8dee559a0"
        },
        "logSet": "HDFS_DATAXCEIVER",
        "actionType": "receiving",
        "ts": {
            "$date": "2008-11-09T20:37:30Z"
        },
        "day": "2008-11-09",
        "sourceIp": "10.251.127.243",
        "destIp": "10.251.127.243",
        "blockId": 6571332664694653000,
        "sizeBytes": null,
        "upvoteCount": 7,
        "ingestKey": "e2915ff85eb4323b14d4f29b246f57423adaa1f1650d30f90d036a9e36234c05"
    },
    {
        "_id": {
            "$oid": "69743a0772096bfb9be57715"
        },
        "logSet": "HDFS_NAMESYSTEM",
        "ingestKey": "a8cbc01f3d23edfa91c27f0e773dc4b10240795571e8aa17b9de2eb52fe6e2ee",
        "actionType": "update",
        "ts": {
            "$date": "2008-11-09T20:42:50Z"
        },
        "day": "2008-11-09",
        "sourceIp": null,
        "destIp": "10.251.214.18",
        "blockId": -7753866805176677000,
        "sizeBytes": 67108864,
        "upvoteCount": 7
    },
    {
        "_id": {
            "$oid": "69743a07333a5ad8dee57b7f"
        },
        "logSet": "HDFS_DATAXCEIVER",
        "actionType": "receiving",
        "ts": {
            "$date": "2008-11-09T20:42:31Z"
        },
        "day": "2008-11-09",
        "sourceIp": "10.251.194.245",
        "destIp": "10.251.194.245",
        "blockId": -1793440627902049000,
        "sizeBytes": null,
        "upvoteCount": 7,
        "ingestKey": "7ba071bc3c36f0246aaa6203d6475195014e5264a925cc57b50af4756e2bbefb"
    },
    {
        "_id": {
            "$oid": "69743a07333a5ad8dee58094"
        },
        "logSet": "HDFS_DATAXCEIVER",
        "actionType": "receiving",
        "ts": {
            "$date": "2008-11-09T20:43:17Z"
        },
        "day": "2008-11-09",
        "sourceIp": "10.251.214.225",
        "destIp": "10.251.214.225",
        "blockId": 4039064252668851700,
        "sizeBytes": null,
        "upvoteCount": 7,
        "ingestKey": "1da99757373d765f253cbb7aabf82b4baeee73471bbdfe7ffc04efcd2d0e6a10"
    },
    {
        "_id": {
            "$oid": "69743a0772096bfb9be58485"
        },
        "logSet": "HDFS_NAMESYSTEM",
        "ingestKey": "4f0f103244e40a50605ceb5997fc359d19f4a832324d210acfc6facba7f0ad73",
        "actionType": "update",
        "ts": {
            "$date": "2008-11-09T20:44:52Z"
        },
        "day": "2008-11-09",
        "sourceIp": null,
        "destIp": "10.251.31.180",
        "blockId": -4974385501486779000,
        "sizeBytes": 67108864,
        "upvoteCount": 7
    },
    {
        "_id": {
            "$oid": "69743a0772096bfb9be58c45"
        },
        "logSet": "HDFS_NAMESYSTEM",
        "ingestKey": "6c89a621471bdbbb6530d35e45838170b02ffd6af7a5364069f8cae01fcf5a8c",
        "actionType": "update",
        "ts": {
            "$date": "2008-11-09T20:45:56Z"
        },
        "day": "2008-11-09",
        "sourceIp": null,
        "destIp": "10.251.111.130",
        "blockId": -8191677345482863000,
        "sizeBytes": 3541870,
        "upvoteCount": 7
    },
    {
        "_id": {
            "$oid": "69743a0772096bfb9be58e76"
        },
        "logSet": "HDFS_NAMESYSTEM",
        "ingestKey": "1b652a869d7d039c41927d566029df57f6df74c0efcf9f2e58577e62dd5baf0b",
        "actionType": "update",
        "ts": {
            "$date": "2008-11-09T20:46:15Z"
        },
        "day": "2008-11-09",
        "sourceIp": null,
        "destIp": "10.251.67.225",
        "blockId": 594983958703228900,
        "sizeBytes": 67108864,
        "upvoteCount": 7
    },
    {
        "_id": {
            "$oid": "69743a07333a5ad8dee5881c"
        },
        "logSet": "HDFS_DATAXCEIVER",
        "actionType": "receiving",
        "ts": {
            "$date": "2008-11-09T20:44:25Z"
        },
        "day": "2008-11-09",
        "sourceIp": "10.251.194.129",
        "destIp": "10.251.194.129",
        "blockId": -4723260765738299000,
        "sizeBytes": null,
        "upvoteCount": 7,
        "ingestKey": "04cd0611f49d8f49287a3d089522cd36de0343afde521fe9ec69f64bded53036"
    },
    {
        "_id": {
            "$oid": "69743a07333a5ad8dee58ab2"
        },
        "logSet": "HDFS_DATAXCEIVER",
        "actionType": "receiving",
        "ts": {
            "$date": "2008-11-09T20:44:48Z"
        },
        "day": "2008-11-09",
        "sourceIp": "10.251.71.193",
        "destIp": "10.251.71.193",
        "blockId": 5447159055488794000,
        "sizeBytes": null,
        "upvoteCount": 7,
        "ingestKey": "2ab192cbb8f4064834fe51068fc6ce35f0df7cd7268235c8413789a506170e51"
    },
    {
        "_id": {
            "$oid": "69743a07333a5ad8dee597bd"
        },
        "logSet": "HDFS_DATAXCEIVER",
        "actionType": "receiving",
        "ts": {
            "$date": "2008-11-09T20:46:34Z"
        },
        "day": "2008-11-09",
        "sourceIp": "10.251.194.245",
        "destIp": "10.251.194.245",
        "blockId": -6213416666354064000,
        "sizeBytes": null,
        "upvoteCount": 7,
        "ingestKey": "573de1820eb85f90c512fc595faab27fbf2835874eb6111a38f2be5c0a27c950"
    },
    {
        "_id": {
            "$oid": "69743a07333a5ad8dee598ea"
        },
        "logSet": "HDFS_DATAXCEIVER",
        "actionType": "receiving",
        "ts": {
            "$date": "2008-11-09T20:46:42Z"
        },
        "day": "2008-11-09",
        "sourceIp": "10.251.198.196",
        "destIp": "10.251.198.196",
        "blockId": -8928239357936099000,
        "sizeBytes": null,
        "upvoteCount": 7,
        "ingestKey": "100ddcb90a588a546022913f921ecaae71284a048529f37fd6b30372180d80c4"
    },
    {
        "_id": {
            "$oid": "69743a0772096bfb9be5a30e"
        },
        "logSet": "HDFS_NAMESYSTEM",
        "ingestKey": "063437b911f6d77add9a69e4ead31d49d3132b76747ea6c2b96fdefb6882eb2c",
        "actionType": "update",
        "ts": {
            "$date": "2008-11-09T20:48:58Z"
        },
        "day": "2008-11-09",
        "sourceIp": null,
        "destIp": "10.251.111.80",
        "blockId": -7140447368456389000,
        "sizeBytes": 67108864,
        "upvoteCount": 7
    },
    {
        "_id": {
            "$oid": "69743a07333a5ad8dee59fc2"
        },
        "logSet": "HDFS_DATAXCEIVER",
        "actionType": "receiving",
        "ts": {
            "$date": "2008-11-09T20:47:34Z"
        },
        "day": "2008-11-09",
        "sourceIp": "10.251.126.22",
        "destIp": "10.251.126.22",
        "blockId": 4246034217575940000,
        "sizeBytes": null,
        "upvoteCount": 7,
        "ingestKey": "b7447c371eb859f14a4da43308913f3f243ff45d6c87d97ddbc6f1cd9a954aac"
    },
    {
        "_id": {
            "$oid": "69743a0772096bfb9be5b1a6"
        },
        "logSet": "HDFS_NAMESYSTEM",
        "ingestKey": "555ebab755189ec988493e75a8a935c0919f31803a6512bdecc53a6e016c1add",
        "actionType": "update",
        "ts": {
            "$date": "2008-11-09T20:51:12Z"
        },
        "day": "2008-11-09",
        "sourceIp": null,
        "destIp": "10.251.123.99",
        "blockId": 1238872000800533000,
        "sizeBytes": 67108864,
        "upvoteCount": 7
    },
    {
        "_id": {
            "$oid": "69743a0772096bfb9be5b3f0"
        },
        "logSet": "HDFS_NAMESYSTEM",
        "ingestKey": "09c671bb1b4fd94a37d3e601c93571fcfbb1e1561d5c1605797261e9e8245b5f",
        "actionType": "update",
        "ts": {
            "$date": "2008-11-09T20:51:32Z"
        },
        "day": "2008-11-09",
        "sourceIp": null,
        "destIp": "10.251.127.191",
        "blockId": 4641520900985057000,
        "sizeBytes": 67108864,
        "upvoteCount": 7
    },
    {
        "_id": {
            "$oid": "69743a07333a5ad8dee5ad47"
        },
        "logSet": "HDFS_DATAXCEIVER",
        "actionType": "receiving",
        "ts": {
            "$date": "2008-11-09T20:49:27Z"
        },
        "day": "2008-11-09",
        "sourceIp": "10.251.73.188",
        "destIp": "10.251.73.188",
        "blockId": -6304893500229432000,
        "sizeBytes": null,
        "upvoteCount": 7,
        "ingestKey": "3b1607d4957a3286ac070dcd3120100ec2cbc126fa671c0c3cc79e301f520794"
    },
    {
        "_id": {
            "$oid": "69743a0772096bfb9be5b6c9"
        },
        "logSet": "HDFS_NAMESYSTEM",
        "ingestKey": "56fde37e39a9f901b70faaf59bcc0506b03a35971778e97f31f078ac7695ded3",
        "actionType": "update",
        "ts": {
            "$date": "2008-11-09T20:51:57Z"
        },
        "day": "2008-11-09",
        "sourceIp": null,
        "destIp": "10.251.201.204",
        "blockId": 5239540619850611000,
        "sizeBytes": 67108864,
        "upvoteCount": 7
    },
    {
        "_id": {
            "$oid": "69743a0772096bfb9be5bd17"
        },
        "logSet": "HDFS_NAMESYSTEM",
        "ingestKey": "0e51d6087fa274f2fd70c24b0222492b5dbbf205782b34297fcd742e6fb4c9d2",
        "actionType": "update",
        "ts": {
            "$date": "2008-11-09T20:52:55Z"
        },
        "day": "2008-11-09",
        "sourceIp": null,
        "destIp": "10.251.66.3",
        "blockId": -8902048459092357000,
        "sizeBytes": 67108864,
        "upvoteCount": 7
    },
    {
        "_id": {
            "$oid": "69743a07333a5ad8dee5b62f"
        },
        "logSet": "HDFS_DATAXCEIVER",
        "actionType": "receiving",
        "ts": {
            "$date": "2008-11-09T20:50:48Z"
        },
        "day": "2008-11-09",
        "sourceIp": "10.251.75.163",
        "destIp": "10.251.75.163",
        "blockId": 7509305879715460000,
        "sizeBytes": null,
        "upvoteCount": 7,
        "ingestKey": "ea70707d4b9f27221950b137471c2563f7b7c6b7ef96d6affd3c992fc23dd55d"
    },
    {
        "_id": {
            "$oid": "69743a0772096bfb9be5c68e"
        },
        "logSet": "HDFS_NAMESYSTEM",
        "ingestKey": "dc4c0ec728115407e15ba7fd3be86e6621d9f690824fb938d351f30237190aa3",
        "actionType": "update",
        "ts": {
            "$date": "2008-11-09T20:54:21Z"
        },
        "day": "2008-11-09",
        "sourceIp": null,
        "destIp": "10.251.30.101",
        "blockId": -5540284612232496000,
        "sizeBytes": 67108864,
        "upvoteCount": 7
    },
    {
        "_id": {
            "$oid": "69743a07333a5ad8dee5c134"
        },
        "logSet": "HDFS_DATAXCEIVER",
        "actionType": "receiving",
        "ts": {
            "$date": "2008-11-09T20:52:26Z"
        },
        "day": "2008-11-09",
        "sourceIp": "10.251.199.159",
        "destIp": "10.251.199.159",
        "blockId": 2898305000422240000,
        "sizeBytes": null,
        "upvoteCount": 7,
        "ingestKey": "166829d70f4264336366fdbe42df9e9579c7fd639b2a4e4ad8812de6fda21ecf"
    },
    {
        "_id": {
            "$oid": "69743a07333a5ad8dee5c1e4"
        },
        "logSet": "HDFS_DATAXCEIVER",
        "actionType": "receiving",
        "ts": {
            "$date": "2008-11-09T20:52:33Z"
        },
        "day": "2008-11-09",
        "sourceIp": "10.251.107.50",
        "destIp": "10.251.107.50",
        "blockId": 3889143725387441700,
        "sizeBytes": null,
        "upvoteCount": 7,
        "ingestKey": "2379ca124bb2d17087c02ec59e4df408ad8e92534d053aad501b8eda3e7db9a2"
    },
    {
        "_id": {
            "$oid": "69743a07333a5ad8dee5c423"
        },
        "logSet": "HDFS_DATAXCEIVER",
        "actionType": "receiving",
        "ts": {
            "$date": "2008-11-09T20:52:52Z"
        },
        "day": "2008-11-09",
        "sourceIp": "10.251.42.84",
        "destIp": "10.251.42.84",
        "blockId": 4840175482129527000,
        "sizeBytes": null,
        "upvoteCount": 7,
        "ingestKey": "70e03ddbd2aaec28fed2298fb5549bd1258014f4e71a3c3f4a933295feb9cd48"
    },
    {
        "_id": {
            "$oid": "69743a0772096bfb9be5d270"
        },
        "logSet": "HDFS_NAMESYSTEM",
        "ingestKey": "d93301f6710f64ba713c49b9fbc40d3380df0351961293d48c11c023370e1f4f",
        "actionType": "update",
        "ts": {
            "$date": "2008-11-09T20:56:08Z"
        },
        "day": "2008-11-09",
        "sourceIp": null,
        "destIp": "10.251.35.1",
        "blockId": -5830384485029959000,
        "sizeBytes": 67108864,
        "upvoteCount": 7
    },
    {
        "_id": {
            "$oid": "69743a0772096bfb9be5d608"
        },
        "logSet": "HDFS_NAMESYSTEM",
        "ingestKey": "51a0b8f60849b09da756177aa931fe05e1f73693f661f0824bf769526cc207fb",
        "actionType": "update",
        "ts": {
            "$date": "2008-11-09T20:56:36Z"
        },
        "day": "2008-11-09",
        "sourceIp": null,
        "destIp": "10.251.214.175",
        "blockId": 5065822136883359000,
        "sizeBytes": 67108864,
        "upvoteCount": 7
    },
    {
        "_id": {
            "$oid": "69743a0772096bfb9be5d75b"
        },
        "logSet": "HDFS_NAMESYSTEM",
        "ingestKey": "60a50e7d6d1654e4bcba0028defb920729d57f49839cd4f03ad4be9a9d8891cc",
        "actionType": "update",
        "ts": {
            "$date": "2008-11-09T20:56:49Z"
        },
        "day": "2008-11-09",
        "sourceIp": null,
        "destIp": "10.251.38.53",
        "blockId": -8645783273050115000,
        "sizeBytes": 67108864,
        "upvoteCount": 7
    },
    {
        "_id": {
            "$oid": "69743a0772096bfb9be5d867"
        },
        "logSet": "HDFS_NAMESYSTEM",
        "ingestKey": "74e8a39aa364479a071428d63ffd5a73bf613d456d53e65586b559aea1695805",
        "actionType": "update",
        "ts": {
            "$date": "2008-11-09T20:56:57Z"
        },
        "day": "2008-11-09",
        "sourceIp": null,
        "destIp": "10.251.110.8",
        "blockId": 8304852181833027000,
        "sizeBytes": 67108864,
        "upvoteCount": 7
    },
    {
        "_id": {
            "$oid": "69743a0772096bfb9be5dc35"
        },
        "logSet": "HDFS_NAMESYSTEM",
        "ingestKey": "55e57cdde22b2b4a1edf35252073b8931f8509355fb3f942ff45eabec3cd5292",
        "actionType": "update",
        "ts": {
            "$date": "2008-11-09T20:57:27Z"
        },
        "day": "2008-11-09",
        "sourceIp": null,
        "destIp": "10.251.91.229",
        "blockId": 3349468155292270000,
        "sizeBytes": 67108864,
        "upvoteCount": 7
    },
    {
        "_id": {
            "$oid": "69743a0772096bfb9be5ded5"
        },
        "logSet": "HDFS_NAMESYSTEM",
        "ingestKey": "aff423872ea6d8b5aead4d12172d0e93d20447b70fb45702b5225a2db55dee55",
        "actionType": "update",
        "ts": {
            "$date": "2008-11-09T20:57:48Z"
        },
        "day": "2008-11-09",
        "sourceIp": null,
        "destIp": "10.251.215.16",
        "blockId": 8280740301759951000,
        "sizeBytes": 67108864,
        "upvoteCount": 7
    },
    {
        "_id": {
            "$oid": "69743a0772096bfb9be5e0b3"
        },
        "logSet": "HDFS_NAMESYSTEM",
        "ingestKey": "3ddad9ccf5e7aa7c5d53719f4cf82215efb9d242e77eccabf8e76acc9202fdf8",
        "actionType": "update",
        "ts": {
            "$date": "2008-11-09T20:58:02Z"
        },
        "day": "2008-11-09",
        "sourceIp": null,
        "destIp": "10.251.43.115",
        "blockId": -8042642627944074000,
        "sizeBytes": 67108864,
        "upvoteCount": 7
    },
    {
        "_id": {
            "$oid": "69743a0772096bfb9be5e221"
        },
        "logSet": "HDFS_NAMESYSTEM",
        "ingestKey": "ce2b52371c5be04f9b06e1f236b32c1c12b5133a0c0c048d6504c47cb231cc03",
        "actionType": "update",
        "ts": {
            "$date": "2008-11-09T20:58:15Z"
        },
        "day": "2008-11-09",
        "sourceIp": null,
        "destIp": "10.251.75.143",
        "blockId": 1755120279523820500,
        "sizeBytes": 67108864,
        "upvoteCount": 7
    },
    {
        "_id": {
            "$oid": "69743a0872096bfb9be5eb19"
        },
        "logSet": "HDFS_NAMESYSTEM",
        "ingestKey": "497d433a402f442bffb625fe7c152de508dd86e703df633e861a8d993abd63b7",
        "actionType": "update",
        "ts": {
            "$date": "2008-11-09T20:59:27Z"
        },
        "day": "2008-11-09",
        "sourceIp": null,
        "destIp": "10.251.199.245",
        "blockId": 8935518525483688000,
        "sizeBytes": 67108864,
        "upvoteCount": 7
    },
    {
        "_id": {
            "$oid": "69743a0872096bfb9be5f8d0"
        },
        "logSet": "HDFS_NAMESYSTEM",
        "ingestKey": "3ef115af934344a16535b2a454a70eb3f9329131d95dc39f68eaca1abd641872",
        "actionType": "update",
        "ts": {
            "$date": "2008-11-09T21:01:25Z"
        },
        "day": "2008-11-09",
        "sourceIp": null,
        "destIp": "10.251.66.3",
        "blockId": -7586733663313576000,
        "sizeBytes": 67108864,
        "upvoteCount": 7
    },
    {
        "_id": {
            "$oid": "69743a08333a5ad8dee5f7a8"
        },
        "logSet": "HDFS_DATAXCEIVER",
        "actionType": "receiving",
        "ts": {
            "$date": "2008-11-09T21:00:07Z"
        },
        "day": "2008-11-09",
        "sourceIp": "10.250.11.53",
        "destIp": "10.250.11.53",
        "blockId": -214224081305579360,
        "sizeBytes": null,
        "upvoteCount": 7,
        "ingestKey": "43448fade90ab86f8f5475ea01e6dfe96cdab9aa1e019c36eda6b2cfe33913b9"
    }
];



/* ============================================================
   Q8. Fifty most active administrators by total upvotes
============================================================ */
print("\nQ8: Top admins by total upvotes");
db.admins.find(
    {},
    { username: 1, email: 1, phone: 1, totalUpvotes: 1 }
).sort({ totalUpvotes: -1 }).limit(50).toArray();
// results
resultsQ8 = [
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80cd"
        },
        "username": "matthew11",
        "email": "browndenise@example.net",
        "phone": "(469)753-4215x67620",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80d5"
        },
        "username": "collinsjaime",
        "email": "john00@example.org",
        "phone": "+1-876-567-2084x297",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80ab"
        },
        "username": "zchristensen",
        "email": "reyesjames@example.net",
        "phone": "(751)979-1918",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80c9"
        },
        "username": "ravenbennett",
        "email": "lisa08@example.net",
        "phone": "8014253703",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80c5"
        },
        "username": "jennifer82",
        "email": "perryjessica@example.com",
        "phone": "(884)651-8969",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80b9"
        },
        "username": "jeffrey93",
        "email": "williamroach@example.org",
        "phone": "+1-805-822-4087x474",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80d1"
        },
        "username": "richardmorse",
        "email": "qpark@example.net",
        "phone": "898.512.6528x437",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80a8"
        },
        "username": "romantonya",
        "email": "llang@example.net",
        "phone": "(465)951-7447x337",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80c1"
        },
        "username": "renee33",
        "email": "wilsoncorey@example.com",
        "phone": "218.396.6557",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80b3"
        },
        "username": "audreywillis",
        "email": "kimandrew@example.net",
        "phone": "(899)603-2311x992",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80c7"
        },
        "username": "theresa96",
        "email": "qcruz@example.org",
        "phone": "(267)404-7320x473",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80c3"
        },
        "username": "yjones",
        "email": "qmurphy@example.net",
        "phone": "261.490.8777x734",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80b7"
        },
        "username": "tommy83",
        "email": "rachelmendez@example.com",
        "phone": "+1-621-615-6390x75404",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80cb"
        },
        "username": "elizabeth01",
        "email": "zmathews@example.com",
        "phone": "311.984.2813x59650",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80af"
        },
        "username": "shannonhester",
        "email": "grahamvictoria@example.org",
        "phone": "(494)319-9245x20676",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80cf"
        },
        "username": "matthew34",
        "email": "hallvictoria@example.com",
        "phone": "425.528.5572",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80bb"
        },
        "username": "ryanwilliam",
        "email": "bonniemeyer@example.net",
        "phone": "(276)553-2084x05164",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80d3"
        },
        "username": "jessica13",
        "email": "hurleyjennifer@example.com",
        "phone": "728.442.1390x3915",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80a7"
        },
        "username": "chad41",
        "email": "hbaird@example.net",
        "phone": "(837)794-4126x016",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80d7"
        },
        "username": "bethany84",
        "email": "whitejennifer@example.com",
        "phone": "467.853.0243x392",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80d8"
        },
        "username": "johnhancock",
        "email": "carmenyates@example.com",
        "phone": "001-350-929-4064x06621",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80c0"
        },
        "username": "heatherphillips",
        "email": "whiteann@example.org",
        "phone": "001-661-709-0457x136",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80ac"
        },
        "username": "amartin",
        "email": "hartjames@example.com",
        "phone": "+1-965-730-5169",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80c2"
        },
        "username": "schneiderjoseph",
        "email": "jbuchanan@example.net",
        "phone": "(857)519-1949",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80b4"
        },
        "username": "robert69",
        "email": "tinaduran@example.net",
        "phone": "001-719-740-2702",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80c6"
        },
        "username": "jared80",
        "email": "billy31@example.org",
        "phone": "578.627.4349x06368",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80c4"
        },
        "username": "ysanders",
        "email": "matthewroberson@example.com",
        "phone": "(979)720-0233x240",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80b6"
        },
        "username": "kimberlyruiz",
        "email": "pperez@example.net",
        "phone": "455-477-6861x73282",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80c8"
        },
        "username": "kristyhampton",
        "email": "teresa72@example.net",
        "phone": "+1-691-553-7951",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80ae"
        },
        "username": "isanders",
        "email": "timothy96@example.net",
        "phone": "298-537-5501x974",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80ca"
        },
        "username": "kaylaharris",
        "email": "david78@example.com",
        "phone": "001-464-962-5064",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80b8"
        },
        "username": "bmoore",
        "email": "nramos@example.net",
        "phone": "(337)989-2984x39876",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80cc"
        },
        "username": "matthew98",
        "email": "anthony35@example.org",
        "phone": "001-251-221-7200x7528",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80aa"
        },
        "username": "ewilson",
        "email": "hvelasquez@example.net",
        "phone": "(885)202-6695x89279",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80ce"
        },
        "username": "catherinehernandez",
        "email": "david56@example.com",
        "phone": "2838270104",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80ba"
        },
        "username": "henrywalker",
        "email": "andrew86@example.net",
        "phone": "+1-628-757-1909x7036",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80d0"
        },
        "username": "kathleenadams",
        "email": "princejoseph@example.net",
        "phone": "+1-225-802-3271x9113",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80b0"
        },
        "username": "vernonkelly",
        "email": "patrick93@example.org",
        "phone": "+1-855-508-5831x3108",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80d2"
        },
        "username": "clementsjohn",
        "email": "eric91@example.com",
        "phone": "(506)517-5703",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80bc"
        },
        "username": "rachel57",
        "email": "kurtcarpenter@example.net",
        "phone": "9513482310",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80b1"
        },
        "username": "hunter93",
        "email": "jennifer40@example.net",
        "phone": "3083354333",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80da"
        },
        "username": "powellkimberly",
        "email": "johnramirez@example.com",
        "phone": "+1-487-936-0920x84280",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80d6"
        },
        "username": "cmartinez",
        "email": "jameswillis@example.org",
        "phone": "836.565.2808x981",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80be"
        },
        "username": "martinezjennifer",
        "email": "harriscandace@example.net",
        "phone": "3368127215",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80dc"
        },
        "username": "vfloyd",
        "email": "jade63@example.org",
        "phone": "001-380-940-7136x6808",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80db"
        },
        "username": "natasha49",
        "email": "grayhunter@example.org",
        "phone": "(584)715-2786",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80bf"
        },
        "username": "jesse65",
        "email": "qgraham@example.com",
        "phone": "997-459-5632x451",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80b2"
        },
        "username": "lyonsjoshua",
        "email": "davidbender@example.org",
        "phone": "(770)263-9292x906",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80a9"
        },
        "username": "lori16",
        "email": "ythomas@example.com",
        "phone": "+1-389-717-0516x39308",
        "totalUpvotes": 1000
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80ad"
        },
        "username": "trevor08",
        "email": "kelseychen@example.org",
        "phone": "001-578-636-7415x210",
        "totalUpvotes": 1000
    }
];


/* ============================================================
   Q9. Top 50 admins by number of distinct source IPs voted
============================================================ */
print("\nQ9: Top admins by distinct source IPs");
db.upvotes.aggregate([
    { $match: { sourceIp: { $ne: null } } },
    { $group: { _id: "$adminId", ips: { $addToSet: "$sourceIp" } } },
    { $project: { ipCount: { $size: "$ips" } } },
    { $sort: { ipCount: -1 } },
    { $limit: 50 },
    { $lookup: { from: "admins", localField: "_id", foreignField: "_id", as: "admin" } },
    { $unwind: "$admin" },
    { $project: { adminId: { $toString: "$_id" }, username: "$admin.username", email: "$admin.email", ipCount: 1 } }
]).toArray();
// results 
resultsQ9 = [
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e82f8"
        },
        "ipCount": 248,
        "adminId": "69743a4e6b229405a33e82f8",
        "username": "david14",
        "email": "sandrabennett@example.com"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e8199"
        },
        "ipCount": 247,
        "adminId": "69743a4e6b229405a33e8199",
        "username": "jhawkins",
        "email": "nroberson@example.net"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e82d8"
        },
        "ipCount": 245,
        "adminId": "69743a4e6b229405a33e82d8",
        "username": "robertcarrillo",
        "email": "jjames@example.net"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e85af"
        },
        "ipCount": 245,
        "adminId": "69743a4e6b229405a33e85af",
        "username": "felicia89",
        "email": "perkinskelly@example.net"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80de"
        },
        "ipCount": 244,
        "adminId": "69743a4e6b229405a33e80de",
        "username": "oneillmegan",
        "email": "joshua69@example.com"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e81be"
        },
        "ipCount": 244,
        "adminId": "69743a4e6b229405a33e81be",
        "username": "medinasteven",
        "email": "hartmancurtis@example.com"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e82e7"
        },
        "ipCount": 244,
        "adminId": "69743a4e6b229405a33e82e7",
        "username": "ifowler",
        "email": "curtisreese@example.com"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e8647"
        },
        "ipCount": 243,
        "adminId": "69743a4e6b229405a33e8647",
        "username": "sheliahays",
        "email": "ccarroll@example.org"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e8317"
        },
        "ipCount": 243,
        "adminId": "69743a4e6b229405a33e8317",
        "username": "martineztracy",
        "email": "porterdavid@example.com"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e8496"
        },
        "ipCount": 243,
        "adminId": "69743a4e6b229405a33e8496",
        "username": "norrisstephanie",
        "email": "ualexander@example.com"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e83c7"
        },
        "ipCount": 242,
        "adminId": "69743a4e6b229405a33e83c7",
        "username": "kathleen70",
        "email": "kelly19@example.org"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e829c"
        },
        "ipCount": 242,
        "adminId": "69743a4e6b229405a33e829c",
        "username": "ashleyhall",
        "email": "williamhall@example.org"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e84f8"
        },
        "ipCount": 242,
        "adminId": "69743a4e6b229405a33e84f8",
        "username": "fberry",
        "email": "beverly04@example.com"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e8535"
        },
        "ipCount": 242,
        "adminId": "69743a4e6b229405a33e8535",
        "username": "stephaniepeterson",
        "email": "raymondsimmons@example.com"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80da"
        },
        "ipCount": 242,
        "adminId": "69743a4e6b229405a33e80da",
        "username": "powellkimberly",
        "email": "johnramirez@example.com"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e83b4"
        },
        "ipCount": 242,
        "adminId": "69743a4e6b229405a33e83b4",
        "username": "smiller",
        "email": "lindamarshall@example.org"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e8328"
        },
        "ipCount": 241,
        "adminId": "69743a4e6b229405a33e8328",
        "username": "llevine",
        "email": "tbennett@example.com"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e8547"
        },
        "ipCount": 241,
        "adminId": "69743a4e6b229405a33e8547",
        "username": "qturner",
        "email": "amy38@example.org"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e863e"
        },
        "ipCount": 241,
        "adminId": "69743a4e6b229405a33e863e",
        "username": "donald39",
        "email": "bradley24@example.com"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e84f3"
        },
        "ipCount": 240,
        "adminId": "69743a4e6b229405a33e84f3",
        "username": "csellers",
        "email": "robert50@example.com"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e83c4"
        },
        "ipCount": 240,
        "adminId": "69743a4e6b229405a33e83c4",
        "username": "josephjennifer",
        "email": "jonesricardo@example.org"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e8103"
        },
        "ipCount": 240,
        "adminId": "69743a4e6b229405a33e8103",
        "username": "longkatelyn",
        "email": "nelsonwilliam@example.org"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80b4"
        },
        "ipCount": 240,
        "adminId": "69743a4e6b229405a33e80b4",
        "username": "robert69",
        "email": "tinaduran@example.net"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e852b"
        },
        "ipCount": 240,
        "adminId": "69743a4e6b229405a33e852b",
        "username": "riverasandra",
        "email": "codylewis@example.net"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e862e"
        },
        "ipCount": 240,
        "adminId": "69743a4e6b229405a33e862e",
        "username": "swelch",
        "email": "rlopez@example.org"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e8302"
        },
        "ipCount": 240,
        "adminId": "69743a4e6b229405a33e8302",
        "username": "dwayneroth",
        "email": "lewisfelicia@example.org"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e8506"
        },
        "ipCount": 240,
        "adminId": "69743a4e6b229405a33e8506",
        "username": "ujohnson",
        "email": "eric60@example.com"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e84c3"
        },
        "ipCount": 240,
        "adminId": "69743a4e6b229405a33e84c3",
        "username": "rodriguezryan",
        "email": "michaelnelson@example.org"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e84ef"
        },
        "ipCount": 240,
        "adminId": "69743a4e6b229405a33e84ef",
        "username": "hthompson",
        "email": "johnwhite@example.org"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e8244"
        },
        "ipCount": 239,
        "adminId": "69743a4e6b229405a33e8244",
        "username": "aguilarnathan",
        "email": "kyle95@example.com"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e822f"
        },
        "ipCount": 239,
        "adminId": "69743a4e6b229405a33e822f",
        "username": "lauren97",
        "email": "lisa65@example.net"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e8458"
        },
        "ipCount": 239,
        "adminId": "69743a4e6b229405a33e8458",
        "username": "dbryant",
        "email": "thompsonbrooke@example.net"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e8272"
        },
        "ipCount": 239,
        "adminId": "69743a4e6b229405a33e8272",
        "username": "shannon21",
        "email": "rcarr@example.org"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e866c"
        },
        "ipCount": 239,
        "adminId": "69743a4e6b229405a33e866c",
        "username": "heatherbarrett",
        "email": "isaiah08@example.net"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e81d7"
        },
        "ipCount": 239,
        "adminId": "69743a4e6b229405a33e81d7",
        "username": "charlesbarrett",
        "email": "michael73@example.com"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e8155"
        },
        "ipCount": 239,
        "adminId": "69743a4e6b229405a33e8155",
        "username": "woodshayley",
        "email": "jonathan54@example.net"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e83f1"
        },
        "ipCount": 239,
        "adminId": "69743a4e6b229405a33e83f1",
        "username": "gdavis",
        "email": "kristacunningham@example.com"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e80b7"
        },
        "ipCount": 239,
        "adminId": "69743a4e6b229405a33e80b7",
        "username": "tommy83",
        "email": "rachelmendez@example.com"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e81ad"
        },
        "ipCount": 239,
        "adminId": "69743a4e6b229405a33e81ad",
        "username": "mark92",
        "email": "brittany60@example.com"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e8566"
        },
        "ipCount": 239,
        "adminId": "69743a4e6b229405a33e8566",
        "username": "haysjason",
        "email": "lynnmyers@example.net"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e83ab"
        },
        "ipCount": 239,
        "adminId": "69743a4e6b229405a33e83ab",
        "username": "diane16",
        "email": "garzajason@example.com"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e85c9"
        },
        "ipCount": 238,
        "adminId": "69743a4e6b229405a33e85c9",
        "username": "cherylgordon",
        "email": "yrose@example.net"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e866e"
        },
        "ipCount": 238,
        "adminId": "69743a4e6b229405a33e866e",
        "username": "gabrielduncan",
        "email": "olester@example.com"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e83bb"
        },
        "ipCount": 238,
        "adminId": "69743a4e6b229405a33e83bb",
        "username": "ramirezronald",
        "email": "reynoldsgeorge@example.org"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e847b"
        },
        "ipCount": 238,
        "adminId": "69743a4e6b229405a33e847b",
        "username": "fflores",
        "email": "lauraluna@example.net"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e85bc"
        },
        "ipCount": 238,
        "adminId": "69743a4e6b229405a33e85bc",
        "username": "karen91",
        "email": "andreaturner@example.com"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e8676"
        },
        "ipCount": 238,
        "adminId": "69743a4e6b229405a33e8676",
        "username": "fgraham",
        "email": "patrick61@example.org"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e837b"
        },
        "ipCount": 238,
        "adminId": "69743a4e6b229405a33e837b",
        "username": "jennifer29",
        "email": "michellestewart@example.net"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e854c"
        },
        "ipCount": 238,
        "adminId": "69743a4e6b229405a33e854c",
        "username": "zdean",
        "email": "stephanie85@example.net"
    },
    {
        "_id": {
            "$oid": "69743a4e6b229405a33e8274"
        },
        "ipCount": 238,
        "adminId": "69743a4e6b229405a33e8274",
        "username": "averyamanda",
        "email": "martinkiara@example.com"
    }
];


/* ============================================================
   Q10. Logs where the same email used with more than one username
============================================================ */
print("\nQ10: Logs where same email used with multiple usernames");
db.upvotes.aggregate([
    {
        $group: {
            _id: { email: "$emailUsed", logId: "$logId" },
            usernames: { $addToSet: "$usernameUsed" }
        }
    },
    { $match: { $expr: { $gt: [{ $size: "$usernames" }, 1] } } },
    {
        $lookup: {
            from: "logs",
            localField: "_id.logId",
            foreignField: "_id",
            as: "logDetails"
        }
    },
    { $unwind: "$logDetails" },
    {
        $project: {
            _id: 0,
            flaggedEmail: "$_id.email",
            logId: { $toString: "$_id.logId" },
            usernamesUsed: "$usernames",
            logContent: "$logDetails"
        }
    }
]).toArray();
// results
resultsQ10 = [
     {
          "flaggedEmail": "alex@alex.al",
          "logId": "6974afd5f04b4864d68be087",
          "usernamesUsed": [
            "alexandra",
            "alex"
          ],
          "logContent": {
            "_id": "6974afd5f04b4864d68be087",
            "logSet": "HDFS_NAMESYSTEM",
            "ingestKey": "57171b35ec4c67cceca4d3a00637604259400b059606ad7fc7eb93882de3caae",
            "actionType": "update",
            "ts": "2008-11-09T20:46:01",
            "day": "2008-11-09",
            "sourceIp": null,
            "destIp": "10.251.125.237",
            "blockId": 6952230797247766000,
            "sizeBytes": 67108864,
            "upvoteCount": 11
        }
     }
];


/* ============================================================
   Q11. Block IDs voted by a given username
   Username: "some_username"
============================================================ */
print("\nQ11: Block IDs voted by usernameUsed='alex'");
db.upvotes.aggregate([
    { $match: { usernameUsed: "alex" } },
    { $unwind: "$blockIds" },
    { $group: { _id: "$blockIds" } },
    { $sort: { _id: 1 } },
    { $project: { _id: 0, blockId: "$_id" } }
]).toArray();
// results
resultsQ11 = [
    {
        "blockId": 6952230797247766000
    }
];

