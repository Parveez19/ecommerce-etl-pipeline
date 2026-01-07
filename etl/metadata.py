# etl/metadata.py

def get_last_processed_ts(cursor, pipeline_name: str):
    cursor.execute(
        """
        SELECT last_processed_ts
        FROM etl_metadata
        WHERE pipeline_name = %s
        """,
        (pipeline_name,)
    )
    row = cursor.fetchone()
    return row[0] if row else None


def update_last_processed_ts(cursor, pipeline_name: str, new_ts):
    cursor.execute(
        """
        UPDATE etl_metadata
        SET last_processed_ts = %s
        WHERE pipeline_name = %s
        """,
        (new_ts, pipeline_name)
    )
