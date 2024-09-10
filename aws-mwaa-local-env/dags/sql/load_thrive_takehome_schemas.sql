INSERT INTO csv_schemas (filename, name_of_column, data_type, primary_key)
VALUES
  ('Customers.csv', 'CUSTOMERID', 'BIGINT', TRUE),
  ('Customers.csv', 'EMAIL', 'VARCHAR(255)', FALSE),
  ('Customers.csv', 'FIRSTNAME', 'VARCHAR(100)', FALSE),
  ('Customers.csv', 'BILLINGPOSTCODE', 'INT', FALSE),
  ('Sales.csv', 'ORDERID', 'BIGINT', TRUE),
  ('Sales.csv', 'CUSTOMERID', 'BIGINT', FALSE),
  ('Sales.csv', 'PREDISCOUNTGROSSPRODUCTSALES', 'FLOAT', FALSE),
  ('Sales.csv', 'ORDERWEIGHT', 'FLOAT', FALSE),
  ('TC_Data.csv', 'TRANS_ID', 'BIGINT', TRUE),
  ('TC_Data.csv', 'TCTYPE', 'VARCHAR(50)', FALSE),
  ('TC_Data.csv', 'CREATEDAT', 'TIMESTAMP', FALSE),
  ('TC_Data.csv', 'EXPIREDAT', 'TIMESTAMP', FALSE),
  ('TC_Data.csv', 'CUSTOMERID', 'BIGINT', FALSE),
  ('TC_Data.csv', 'ORDERID', 'BIGINT', FALSE),
  ('TC_Data.csv', 'AMOUNT', 'FLOAT', FALSE),
  ('TC_Data.csv', 'REASON', 'VARCHAR(255)', FALSE)
ON CONFLICT (filename, name_of_column) DO NOTHING;
