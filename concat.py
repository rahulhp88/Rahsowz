UPDATE_QUERY = """
UPDATE `final_table` 
SET enddate = prevraydate 
WHERE enddate = '9999-12-31' 
AND datapK IN 
(SELECT col4 
 FROM `stage_table` 
 WHERE col1={record_identifier} AND flag IN('D', 'U'))
"""
