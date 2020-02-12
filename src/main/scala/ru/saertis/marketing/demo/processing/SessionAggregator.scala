package ru.saertis.marketing.demo.processing

import java.util.UUID

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StringType, StructType}

class SessionAggregator extends UserDefinedAggregateFunction {
    
    override def inputSchema: StructType = {
        new StructType().add("sessionId", StringType, nullable = true)
    }
    
    override def bufferSchema: StructType = {
        new StructType().add("sessionId", StringType, nullable = true)
    }
    
    override def dataType: DataType = StringType
    
    override def deterministic: Boolean = true
    
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        println(s">>> initialize (buffer: $buffer)")
        buffer(0) = UUID.randomUUID().toString
    }
    
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        println(s">>> update (buffer: $buffer -> input: $input)")
        val event = input.getString(0)
        buffer(0) = event match {
            case "app_open" => UUID.randomUUID().toString
            case _ => buffer.getString(0)
        }
    }
    
    override def merge(buffer: MutableAggregationBuffer, row: Row): Unit = {
        println(s">>> merge (buffer: $buffer -> row: $row)")
        buffer(0) = buffer.getString(0)
    }
    
    override def evaluate(buffer: Row): Any = {
        println(s">>> evaluate (buffer: $buffer)")
        buffer.getString(0)
    }
}
