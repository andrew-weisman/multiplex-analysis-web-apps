USE ROLE data_apps_andrewweisman_role;

ALTER SERVICE data_app_db.app_runtime_schema.frontend_service_andrewweisman SUSPEND;
-- Needed for this: GRANT OPERATE ON SERVICE data_app_db.app_runtime_schema.frontend_service_andrewweisman TO ROLE data_apps_andrewweisman_role;

SELECT data_app_db.app_runtime_schema.job_service_andrewweisman_job_id_<JOB_ID>!SPCS_CANCEL_JOB();
-- Needed for this: GRANT OPERATE ON SERVICE data_app_db.app_runtime_schema.job_service_andrewweisman_job_id_<JOB_ID> TO ROLE data_apps_andrewweisman_role;

ALTER COMPUTE POOL data_app_frontend_compute_pool_andrewweisman SUSPEND;
-- Needed for this: GRANT OPERATE ON COMPUTE POOL data_app_frontend_compute_pool_andrewweisman TO ROLE data_apps_andrewweisman_role;

ALTER COMPUTE POOL data_app_workers_compute_pool_andrewweisman SUSPEND;
-- Needed for this: GRANT OPERATE ON COMPUTE POOL data_app_workers_compute_pool_andrewweisman TO ROLE data_apps_andrewweisman_role;
