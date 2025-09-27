from pyspark.sql.functions import unix_timestamp, from_unixtime, col, lit, to_timestamp

def normalize_time(colname):
    """
    Scale timestamp column từ [old_min, old_max] sang [new_min, new_max].
    
    colname: Column hoặc string tên cột
    old_min, old_max, new_min, new_max: string timestamp ("yyyy-MM-dd HH:mm:ss")
    """
    old_min="2100-07-25 00:00:00.000"
    old_max="2220-07-26 16:00:00.000"
    new_min="2000-07-25 00:00:00.000"
    new_max="2025-07-26 16:00:00.000"

    return to_timestamp(
        from_unixtime(
            unix_timestamp(to_timestamp(lit(new_min), "yyyy-MM-dd HH:mm:ss.SSS")) +
            (
                (unix_timestamp(col(colname)) - unix_timestamp(to_timestamp(lit(old_min), "yyyy-MM-dd HH:mm:ss.SSS"))) /
                (unix_timestamp(to_timestamp(lit(old_max), "yyyy-MM-dd HH:mm:ss.SSS")) - unix_timestamp(to_timestamp(lit(old_min), "yyyy-MM-dd HH:mm:ss.SSS")))
            ) * (
                unix_timestamp(to_timestamp(lit(new_max), "yyyy-MM-dd HH:mm:ss.SSS")) - unix_timestamp(to_timestamp(lit(new_min), "yyyy-MM-dd HH:mm:ss.SSS"))
            )
        )
    )
