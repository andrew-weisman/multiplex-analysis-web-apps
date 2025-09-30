USE ROLE data_apps_andrewweisman_role;

ALTER SERVICE data_app_db.app_runtime_schema.frontend_service_andrewweisman RESUME;
-- Needed for this: GRANT OPERATE ON SERVICE data_app_db.app_runtime_schema.frontend_service_andrewweisman TO ROLE data_apps_andrewweisman_role;

SHOW ENDPOINTS IN SERVICE data_app_db.app_runtime_schema.frontend_service_andrewweisman;
-- Current permissions seem to allow this without any special grants.

ALTER COMPUTE POOL data_app_workers_compute_pool_andrewweisman RESUME;
-- Needed for this: GRANT OPERATE ON COMPUTE POOL data_app_workers_compute_pool_andrewweisman TO ROLE data_apps_andrewweisman_role;
