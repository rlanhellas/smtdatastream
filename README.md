## Kafka Connect SMT made by DataStream

### RemoveBackslash
Goal: Remove all backslash (\) from field names

Example on how to add to your connector:
```
transforms=removebackslash
transforms.removebackslash.type=br.com.datastream.kafka.connect.smt.RemoveBackslash$Value
```