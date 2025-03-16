#!/bin/bash

# Check if log file path is provided
if [ $# -ne 1 ]; then
    echo "Usage: $0 <path-to-log-file>"
    exit 1
fi

LOG_FILE=$1

# Create the log file if it doesn't exist
if [ ! -f "$LOG_FILE" ]; then
    echo "Creating new log file: $LOG_FILE"
    touch "$LOG_FILE"
fi

# Function to generate a random log entry
generate_log_entry() {
    local timestamp=$(date "+%Y-%m-%d %H:%M:%S,%3N")
    local levels=("INFO" "WARN" "ERROR" "DEBUG")
    local level=${levels[$RANDOM % 4]}
    local component="org.apache.hadoop.hdfs.server."
    local components=("namenode.NameNode" "namenode.FSNamesystem" "datanode.DataNode" "blockmanagement.BlockManager")
    local comp=${components[$RANDOM % 4]}
    
    local messages=(
        "Processing block blk_$RANDOM$RANDOM"
        "Received block $RANDOM$RANDOM from /192.168.1.$((RANDOM % 100))"
        "Verification succeeded for block BP-$RANDOM-$RANDOM"
        "Served block to /192.168.1.$((RANDOM % 100))"
        "Created file /user/hadoop/data_$RANDOM.txt"
        "Deleted file /tmp/hadoop_$RANDOM.tmp"
        "Replication factor change request received"
        "Safe mode is $((RANDOM % 2 ? "ON" : "OFF"))"
        "DataNode registration from 192.168.1.$((RANDOM % 100))"
        "Total blocks: $((100 + RANDOM % 900)) Total files: $((42 + RANDOM % 100))"
    )
    local message=${messages[$RANDOM % ${#messages[@]}]}
    
    echo "$timestamp $level $component$comp: $message"
}

# Initially populate the file with the sample log content
if [ ! -s "$LOG_FILE" ]; then
    cat > "$LOG_FILE" << 'EOL'
2025-03-16 08:00:15,234 INFO org.apache.hadoop.hdfs.server.namenode.NameNode: STARTUP_MSG: Starting NameNode
2025-03-16 08:00:15,987 INFO org.apache.hadoop.hdfs.server.namenode.NameNode: STARTUP_MSG: host = namenode01.hadoop.local/192.168.1.10
2025-03-16 08:00:16,123 INFO org.apache.hadoop.hdfs.server.namenode.NameNode: STARTUP_MSG: args = [-format]
2025-03-16 08:00:16,456 INFO org.apache.hadoop.hdfs.server.common.Storage: Storage directory /data/hadoop/hdfs/name has been successfully formatted.
2025-03-16 08:00:17,890 INFO org.apache.hadoop.hdfs.server.namenode.FSEditLog: Edit logging is enabled
2025-03-16 08:00:18,234 INFO org.apache.hadoop.hdfs.server.namenode.FSNamesystem: Scheduled a checkpoint for transaction ID 0
2025-03-16 08:00:20,567 INFO org.apache.hadoop.hdfs.server.namenode.NameNode: NameNode RPC up at: namenode01.hadoop.local/192.168.1.10:8020
2025-03-16 08:00:21,789 INFO org.apache.hadoop.hdfs.server.namenode.FSNamesystem: Starting services required for active state
2025-03-16 08:00:22,345 INFO org.apache.hadoop.hdfs.server.namenode.NameNode: NameNode up at: http://namenode01.hadoop.local:9870
EOL
    echo "Initialized log file with sample data"
fi

echo "Starting to append log entries every 3-7 seconds. Press Ctrl+C to stop."

# Continuously append new log entries
while true; do
    # Generate 1-3 log entries
    num_entries=$((1 + RANDOM % 3))
    for ((i=1; i<=num_entries; i++)); do
        log_entry=$(generate_log_entry)
        echo "$log_entry" >> "$LOG_FILE"
        echo "Added: $log_entry"
    done
    
    # Wait for 3-7 seconds before adding more entries
    sleep_time=$((3 + RANDOM % 5))
    echo "Waiting $sleep_time seconds..."
    sleep $sleep_time
done
