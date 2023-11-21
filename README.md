Kafka Connect SMT to remove Backslash from field names on json


Example on how to add to your connector:
```
transforms=removebackslash
transforms.removebackslash.type=br.com.datastream.kafka.connect.smt.RemoveBackslash$Value
```