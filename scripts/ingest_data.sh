#!/bin/bash
# Usage: chmod +x ingest.sh && ./ingest.sh
export HADOOP_HOME=/home/hadoop/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# ===============================================
# 1. CONFIGURATION VARIABLES
# ===============================================

FILES=(
    "https://data-engineer-edvai-public.s3.amazonaws.com/2021-informe-ministerio.csv"
    "https://data-engineer-edvai-public.s3.amazonaws.com/202206-informe-ministerio.csv"
    "https://data-engineer-edvai-public.s3.amazonaws.com/aeropuertos_detalle.csv"
)

TEMP_LOCAL_DIR="/tmp/ingest_aereo"
HDFS_DESTINATION="/home/hadoop/landing/aereo"

# ===============================================
# 2. ENVIRONMENT SETUP
# ===============================================

echo "Cleaning and preparing local directory: ${TEMP_LOCAL_DIR}"
rm -rf "${TEMP_LOCAL_DIR}"
mkdir -p "${TEMP_LOCAL_DIR}"
echo "Local directory ready."

echo "Cleaning destination directory in HDFS (Removing ${HDFS_DESTINATION})..."
hdfs dfs -rm -r -skipTrash "${HDFS_DESTINATION}" 2>/dev/null
echo "Verifying/Creating HDFS destination directory: ${HDFS_DESTINATION}"
hdfs dfs -mkdir -p "${HDFS_DESTINATION}"
echo "HDFS (Landing Zone) ready."

# ===============================================
# 3. INGESTION PROCESS
# ===============================================

for FILE_URL in "${FILES[@]}"; do
    FILE_NAME=$(basename "${FILE_URL}")
    LOCAL_FILE="${TEMP_LOCAL_DIR}/${FILE_NAME}"

    echo "--- Processing file: ${FILE_NAME} ---"

    echo "1. Downloading from: ${FILE_URL}..."
    wget --no-check-certificate -q --timeout=120 "${FILE_URL}" -O "${LOCAL_FILE}"

    if [ $? -ne 0 ]; then
        echo "ERROR: Failed to download ${FILE_NAME}. Exiting."
        rm -f "${LOCAL_FILE}"
        exit 1
    fi
    echo "   Download completed at ${LOCAL_FILE}."

    echo "2. Ingesting ${FILE_NAME} to HDFS at ${HDFS_DESTINATION}..."
    hdfs dfs -put -f "${LOCAL_FILE}" "${HDFS_DESTINATION}"

    echo "   Ingestion completed."

    echo "3. Cleaning local file to free up space..."
    rm "${LOCAL_FILE}"
    echo "   Local cleanup for ${FILE_NAME} finished."
done

echo ""
echo "======================================================="
echo "SUCCESS: Ingestion of all files completed."
echo "Files available in HDFS at: ${HDFS_DESTINATION}"
echo "======================================================="

# ===============================================
# 4. FINAL VERIFICATION
# ===============================================

echo "--- Verifying final files in HDFS ---"
hdfs dfs -ls "${HDFS_DESTINATION}"
