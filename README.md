# aws_glue_examples

## Add First Entry of An Array as New Field
```
DFRaw.printSchema
```
```
root
|-- answerEntry: struct
|    |-- sichtbarkeitsindex: array
|    |    |-- element: struct
|    |    |    |-- domain: string
|    |    |    |-- date: string
|    |    |    |-- value: choice
|    |    |    |    |-- double
|    |    |    |    |-- int
```

```
def addFirstEntryAsField(field: String, newField: String): DynamicRecord => DynamicRecord = { record =>
  record.getFieldNode(field) match {
    case Some(arr: ArrayNode) => {
      val fisrtEntryOption = arr.get(0)
      fisrtEntryOption match {
        case Some(entry) => record.addField(newField, entry)
        case _ =>
      }
    }
    case _ =>
  }
  record
}

val udf = addFirstEntryAsField("answerStruct.sichtbarkeitsindex", "sichtbarkeitStruct")
val DFNew = DFRaw.map(f=udf)


DFNew.printSchema
```
```
root
|-- answerEntry: struct
|    |-- sichtbarkeitsindex: array
|    |    |-- element: struct
|    |    |    |-- domain: string
|    |    |    |-- date: string
|    |    |    |-- value: choice
|    |    |    |    |-- double
|    |    |    |    |-- int
|-- sichtbarkeitStruct: struct
|    |-- domain: string
|    |-- date: string
|    |-- value: choice
|    |    |-- double
|    |    |-- int
```
