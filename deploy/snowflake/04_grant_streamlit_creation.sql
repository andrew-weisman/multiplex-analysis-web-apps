-- From Andrew: Probably modify the schema away from my already-created NIH schema. STREAMLIT_APPS is probably a good database name though.

-- Streamlit apps are schema-level objects in Snowflake. 
-- Therefore, they are located in a schema under a database.
-- They also rely on virtual warehouses to provide the compute resource.
-- We recommend starting with X-SMALL warehouses and upgrade when needed.

-- To help your team create Streamlit apps successfully, consider running the following script.
-- Please note that this is an example setup. 
-- You can modify the script to suit your needs.

use role ACCOUNTADMIN;

-- If you want to create a new database for Streamlit Apps, run
CREATE DATABASE STREAMLIT_APPS;
-- If you want to create a specific schema under the database, run
CREATE SCHEMA NIH;
-- Or, you can use the PUBLIC schema that was automatically created with the database.

-- If you want all roles to create Streamlit apps in the PUBLIC schema, run
GRANT USAGE ON DATABASE STREAMLIT_APPS TO ROLE data_apps_andrewweisman_role;
GRANT USAGE ON SCHEMA STREAMLIT_APPS.NIH TO ROLE data_apps_andrewweisman_role;
GRANT CREATE STREAMLIT ON SCHEMA STREAMLIT_APPS.NIH TO ROLE data_apps_andrewweisman_role;
GRANT CREATE STAGE ON SCHEMA STREAMLIT_APPS.NIH TO ROLE data_apps_andrewweisman_role;

-- Don't forget to grant USAGE on a warehouse.
GRANT USAGE ON WAREHOUSE STREAMLIT_APPS_WAREHOUSE TO ROLE data_apps_andrewweisman_role;

-- If you only want certain roles to create Streamlit apps, 
-- or want to enable a different location to store the Streamlit apps,
-- change the database, schema, and role names in the above commands.