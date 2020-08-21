val regex="""\[(\S*\:\S*|\d*\.\d*\.\d*\.\d*)\] - (\d*\.\d*\.\d*\.\d*) - \[(\d*?\S*?\S*\:\d*\:\d*\:\d*) \+\d*\] \"(\S*) (\S*) (\S*)\" (\d*) (\d*) \"\-\" \"(\S*)\"\"(\d*\.\d*\.\d*\.\d*\:\d*)\" \"(\S*)\" \"(\S*)\" .*$"""

import org.apache.spark.sql.functions.{col, udf}
val regexp_split=udf((value : String, regex: String) => { regex.r.unapplySeq(value)})
df.select(regexp_split(col("value"),lit(regex)).as("data")).show(false)
