## Kafka Connect SMT made by DataStream

### RemoveBackslash
Goal: Remove any desired string from field name

Example on how to add to your connector:
```
transforms=removestring
transforms.removestring.type=br.com.datastream.kafka.connect.smt.RemoveStringFieldName$Value
transforms.removestring.string.to.remove=/
```