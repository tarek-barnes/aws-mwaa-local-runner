CREATE TABLE IF NOT EXISTS csv_schemas (
  filename VARCHAR(255),
  name_of_column VARCHAR(255),
  data_type VARCHAR(50),
  primary_key BOOLEAN,
  UNIQUE(filename, name_of_column)
  )
