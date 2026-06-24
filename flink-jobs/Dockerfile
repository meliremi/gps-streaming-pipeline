FROM apache/flink:1.18.1

# Copier les JARs connecteurs (téléchargés localement dans flink-jobs/lib/)
# et les fichiers SQL
COPY lib/ /opt/flink/lib/
COPY sql/ /opt/flink/sql/
