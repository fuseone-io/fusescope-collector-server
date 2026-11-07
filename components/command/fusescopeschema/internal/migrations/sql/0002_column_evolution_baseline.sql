-- data
INSERT INTO {db:metadata}.distributed_column_evolution_metadata (signal, column_name, column_type, field_context, field_name, version, release_time)
SELECT signal, column_name, column_type, field_context, field_name, version, if(version = 0, 0., toFloat64(toUnixTimestamp64Nano(now64(9))))
FROM values('signal String, column_name String, column_type String, field_context String, field_name String, version UInt32',
    ('logs', 'resource', 'JSON()', 'resource', '__all__', 1),
    ('logs', 'resources_string', 'Map(LowCardinality(String), Float64)', 'resource', '__all__', 0),
    ('traces', 'attributes', 'JSON()', 'attribute', '__all__', 1),
    ('traces', 'resource', 'JSON()', 'resource', '__all__', 1),
    ('traces', 'resources_string', 'Map(LowCardinality(String), Float64)', 'resource', '__all__', 0))
WHERE (signal, column_name, column_type, field_context, field_name, version) NOT IN (
    SELECT signal, column_name, column_type, field_context, field_name, version
    FROM {db:metadata}.distributed_column_evolution_metadata)
;
