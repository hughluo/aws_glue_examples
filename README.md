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

## Different Aggregate for Different Field
```
import com.amazonaws.services.glue._
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, min, max, mean, sum}
import org.apache.spark.sql.Column
```

```
scala > rawDyF.printSchema


root
|-- traffic_sum: int
|-- price_sum: double
|-- company_name: string
|-- year: string
|-- month: string
```


```
val rawDaF = rawDyF.toDF()

val mapping: Map[String, Column => Column] = Map("min" -> min, "max" -> max, "mean" -> avg, "sum" -> sum)
val groupBy = Seq("company_name", "year")
val aggregate = Seq("traffic_sum", "price_sum")
val operations = Seq("min", "max", "mean", "sum")
val exprs = aggregate.flatMap(c => operations .map(f => mapping(f)(col(c))))
val aggredDaF = rawDaF.groupBy(groupBy.map(col): _*).agg(exprs.head, exprs.tail: _*)

```

```
scala > aggredDyF.printSchema


root
|-- company_name: string
|-- year: string
|-- min(traffic_sum): int
|-- max(traffic_sum): int
|-- avg(traffic_sum): double
|-- sum(traffic_sum): long
|-- min(price_sum): double
|-- max(price_sum): double
|-- avg(price_sum): double
|-- sum(price_sum): double
```
