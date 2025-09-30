-- Relevant objects: services and compute pools.

USE ROLE data_apps_andrewweisman_role;

DESCRIBE SERVICE data_app_db.app_runtime_schema.frontend_service_andrewweisman;
-- Needed for this: GRANT MONITOR ON SERVICE data_app_db.app_runtime_schema.frontend_service_andrewweisman TO ROLE data_apps_andrewweisman_role;

DESCRIBE SERVICE data_app_db.app_runtime_schema.job_service_andrewweisman_job_id_<JOB_ID>;
-- Needed for this: GRANT MONITOR ON SERVICE data_app_db.app_runtime_schema.job_service_andrewweisman_job_id_<JOB_ID> TO ROLE data_apps_andrewweisman_role;

DESCRIBE COMPUTE POOL data_app_frontend_compute_pool_andrewweisman;
-- Needed for this: GRANT MONITOR ON COMPUTE POOL data_app_frontend_compute_pool_andrewweisman TO ROLE data_apps_andrewweisman_role;

DESCRIBE COMPUTE POOL data_app_workers_compute_pool_andrewweisman;
-- Needed for this: GRANT MONITOR ON COMPUTE POOL data_app_workers_compute_pool_andrewweisman TO ROLE data_apps_andrewweisman_role;
