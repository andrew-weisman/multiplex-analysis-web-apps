-- As ACCOUNTADMIN (or a role with necessary privileges)
USE ROLE ACCOUNTADMIN;

-- (If not already created)
CREATE ROLE IF NOT EXISTS data_apps_andrewweisman_role;

GRANT MONITOR ON WAREHOUSE data_app_warehouse TO ROLE data_apps_andrewweisman_role;

GRANT USAGE ON DATABASE data_app_db TO ROLE data_apps_andrewweisman_role;
GRANT USAGE ON SCHEMA data_app_db.app_runtime_schema TO ROLE data_apps_andrewweisman_role;

-- Note we must provide grants in-app to the job worker service since we don't know the <JOB_ID>s ahead of time.
GRANT MONITOR, OPERATE ON SERVICE data_app_db.app_runtime_schema.frontend_service_andrewweisman TO ROLE data_apps_andrewweisman_role; -- possibly also need USAGE to allow "describe"

GRANT MONITOR, OPERATE ON COMPUTE POOL data_app_frontend_compute_pool_andrewweisman TO ROLE data_apps_andrewweisman_role;

GRANT MONITOR, OPERATE ON COMPUTE POOL data_app_workers_compute_pool_andrewweisman TO ROLE data_apps_andrewweisman_role;

-- Web endpoint access
GRANT SERVICE ROLE
  data_app_db.app_runtime_schema.frontend_service_andrewweisman!web_endpoint_service_role
  TO ROLE data_apps_andrewweisman_role;

-- Assign role to the user
GRANT ROLE data_apps_andrewweisman_role TO USER andrewweisman;

-------------------------------------------------
-- Since I do not want myself (an admin) to have data_app_role, I temporarily granted it to myself in 01_set_up_non_user_objects.sql to do the setup, and am now revoking it. Note data_app_role isn't currently used in this .sql file, so I could put this revokation earlier, but more logical to do it here at the very end of the full setup.
USE ROLE SECURITYADMIN;
REVOKE ROLE data_app_role FROM USER andrewweisman;
-------------------------------------------------

-- Now, as some user (such as janedoe):
--   1. Have an admin temporarily grant me, janedoe, the data_apps_andrewweisman_role, e.g.: GRANT ROLE data_apps_andrewweisman_role TO USER jandoe;
--   2. Grant role data_apps_andrewweisman_role to create Streamlit apps using the *example* in 04_grant_streamlit_creation.sql.
--   3. janedoe creates a launcher app using that role (see launcher.py for the launcher as of 2025-09-05 1807).
--   4. The admin revokes that role from janedoe: REVOKE ROLE data_apps_andrewweisman_role FROM USER janedoe;
-- Then, andrewweisman can use that role, see the app, and launch it with appropriate owner's rights.
  