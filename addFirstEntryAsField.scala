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
