rm nohup.out

nohup spark-submit --packages com.databricks:spark-avro_2.11:3.2.0 attunity_parser.py "2017-09-07 03:00:00"  "attunity-l2" &


