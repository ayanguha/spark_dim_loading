rm nohup.out

#nohup spark-submit --packages com.databricks:spark-avro_2.11:3.2.0 attunity_parser.py wasb://svocdevbdp-eventhub-dc-02@poystdragdlbdp02.blob.core.windows.net/svocdevbdpeventhub/svocdevbdp_eventhub_dc_02/*/2017/07/*/*/*  &
nohup spark-submit --packages com.databricks:spark-avro_2.11:3.2.0 ../attunity_parser.py "2017-07-18 04:00:00"  "attunity-l2"  &

