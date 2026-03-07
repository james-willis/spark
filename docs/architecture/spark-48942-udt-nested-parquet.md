# SPARK-48942: Regression Test for Array of Structs with UDTs in Parquet

## Problem Statement

SPARK-48942 reported that reading Parquet files containing an `ArrayType` of `StructType` where the struct has `UserDefinedType` (UDT) fields throws an `IllegalArgumentException`. The root cause was that UDT resolution only happened at the top level of types, not recursively through nested type trees.

**Error:**
```
java.lang.IllegalArgumentException: Spark type: StructType(StructField(col1,BinaryType,true))
  doesn't match the type: StructType(StructField(col1,GeometryUDT,true)) in column vector
    at ParquetColumnVector.<init>(ParquetColumnVector.java:69)
```

## Root Cause (Already Fixed)

The bug was caused by two code locations that only handled UDTs at the top level:

1. **`ColumnVector` constructor** (`sql/catalyst/src/main/java/org/apache/spark/sql/vectorized/ColumnVector.java`):
   - Before fix: `if (type instanceof UserDefinedType) { this.type = ((UserDefinedType) type).sqlType(); }`
   - After fix: `this.type = type.transformRecursively(...)` — recursively resolves UDTs at any depth

2. **`ParquetSchemaConverter.convertField`** (`sql/core/src/main/scala/.../parquet/ParquetSchemaConverter.scala`):
   - Before fix: Pattern match on `case udt: UserDefinedType[_] => udt.sqlType` (top-level only)
   - After fix: `_.transformRecursively { case t: UserDefinedType[_] => t.sqlType }` (recursive)

Both were fixed in **SPARK-52651** (commit `0c6d7fd277b`, July 3 2025) by Kent Yao.

## Goals

- Add a regression test that covers the **exact** SPARK-48942 scenario: `ArrayType(StructType(StructField("col1", SomeUDT)))` — an array of structs where the struct *contains* a UDT field
- Verify that both the vectorized and non-vectorized parquet readers handle this correctly
- Ensure the test writes data to parquet and reads it back, matching the real-world workflow from the bug report

## Non-Goals

- Backporting to older branches (3.5, 4.0)
- Adding new UDT types or modifying the fix itself

## Design

### Test Location

Add the test to `ParquetQuerySuite.scala` alongside the existing SPARK-39086 UDT tests (around line 842). This file already has:
- The `TestNestedStructUDT`, `TestPrimitiveUDT`, and `TestArrayUDT` test UDT types
- The `withAllParquetReaders` helper for testing both vectorized and non-vectorized readers
- Existing UDT-related parquet tests as reference

### Test Structure

```scala
test("SPARK-48942: Array of Structs with UDT fields in Parquet") {
  // Schema: ArrayType(StructType(StructField("col1", TestPrimitiveUDT)))
  // This is the exact pattern from the bug: ARRAY(STRUCT(udt_column))
  val schema = StructType(
    StructField("arr", ArrayType(
      StructType(Seq(
        StructField("col1", new TestPrimitiveUDT())
      ))
    )) :: Nil)

  withTempPath { dir =>
    // Create data matching the schema
    val rows = sparkContext.parallelize(0 until 2).map { i =>
      Row(Seq(Row(TestPrimitive(i + 1))))
    }
    val df = spark.createDataFrame(rows, schema)
    df.write.parquet(dir.getCanonicalPath)

    // Read back with both vectorized and non-vectorized readers
    for (offHeapEnabled <- Seq(true, false)) {
      withSQLConf(SQLConf.COLUMN_VECTOR_OFFHEAP_ENABLED.key -> offHeapEnabled.toString) {
        withAllParquetReaders {
          val res = spark.read.parquet(dir.getCanonicalPath)
          checkAnswer(res, df)
        }
      }
    }
  }
}
```

### Coverage Analysis

| Nesting Pattern | Existing Test | Notes |
|---|---|---|
| `UDT` (top-level) | ✅ SPARK-39086 | Direct UDT as column type |
| `ArrayType(UDT)` | ✅ SPARK-39086 | Array where element IS a UDT |
| `StructType(UDT field)` | ✅ SPARK-39086 | Struct containing UDT field |
| `MapType(_, UDT)` | ✅ SPARK-52651 | Map with UDT value |
| **`ArrayType(StructType(UDT field))`** | ❌ **MISSING** | **SPARK-48942 scenario** |
| `MapType(_, StructType(UDT field))` | ❌ Missing | Not reported, but same root cause |

The regression test should cover at minimum the bolded row. Optionally also add `MapType(_, StructType(UDT field))` for completeness.

### Also add to ColumnVectorSuite

Add a unit test to `ColumnVectorSuite.scala` for the `ArrayType(StructType(UDT field))` pattern to verify the type resolution at the ColumnVector level:

```scala
testVectors("user defined type in array of struct type",
  10, ArrayType(StructType(Seq(StructField("year", yearUDT))))) { testVector =>
  assert(testVector.dataType() ===
    ArrayType(StructType(Seq(StructField("year", IntegerType)))))
}
```

## Risks

- **Low risk**: This is a test-only change; no production code is modified
- The existing fix (SPARK-52651) handles this case via `transformRecursively` which is inherently recursive through all nesting levels

## Open Questions

None — this is a straightforward regression test addition.
