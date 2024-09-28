"""
SELECT "وقت السحب" FROM {dim_table} d
LEFT JOIN {fact_table} f On f."إعلان رقم" = d."إعلان رقم" 
ORDER BY d."وقت السحب" LIMIT 1
"""