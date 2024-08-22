package io.exsql.bytegraph

import io.exsql.bytegraph.metadata.ByteGraphSchema.{ByteGraphReferenceType, ByteGraphSchema, ByteGraphSchemaField}

import scala.collection.mutable

package object codegen {

  private[bytegraph] val encodedAssertedGraphFieldName = "encoded_asserted_graph"

  private[bytegraph] val partitionIdFieldName = "_partition_id"

  private[bytegraph] def isExternalField(field: ByteGraphSchemaField): Boolean = {
    field.name == encodedAssertedGraphFieldName || field.name == partitionIdFieldName
  }

  private[bytegraph] def extractReadColumnPositions(readSchema: ByteGraphSchema,
                                                    schema: ByteGraphSchema): mutable.LinkedHashMap[Int, ByteGraphSchemaField] = {

    val fieldIndex = mutable.LinkedHashMap.empty[Int, ByteGraphSchemaField]
    readSchema.fields.foreach { field =>
      field.byteGraphDataType match {
        case byteGraphReferenceType: ByteGraphReferenceType =>
          fieldIndex += schema.fieldIndexLookup(field.name) -> schema.fieldLookup(byteGraphReferenceType.field)

        case _ => fieldIndex += schema.fieldIndexLookup(field.name) -> field
      }
    }

    fieldIndex
  }

}
