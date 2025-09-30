-- Will run from data_app_role via the app itself.

USE ROLE data_app_role;

-- vars in spec: <JOB_ID>, <USERNAME>
EXECUTE JOB SERVICE
    IN COMPUTE POOL data_app_workers_compute_pool_<USERNAME>
    NAME = data_app_db.app_runtime_schema.job_service_<USERNAME>_job_id_<JOB_ID>
    ASYNC = TRUE
    FROM SPECIFICATION $$
spec:
  containers:                           # container list
    - name: worker
      image: /data_app_db/app_runtime_schema/image_repository/data_app:latest
      command:                            # optional list of strings
        - conda
        - run
        - -n
        - full_stack_data_app
        - python
        - -c
        - "import analysis_framework; analysis_framework.run_local_analysis('<JOB_ID>')"
      env:                                # optional
        SNOWFLAKE_USER: <USERNAME>             # must inject manually; Snowflake does not inject
        SNOWFLAKE_WAREHOUSE: data_app_warehouse   # must inject manually; Snowflake does not inject
        TZ: America/New_York
        ARCHIVES_BUCKET_NAME: archives
        JOB_INPUTS_BUCKET_NAME: inputs
        JOB_OUTPUTS_BUCKET_NAME: outputs
        APP_PLATFORM: snowflake
      volumeMounts:                       # optional list
        - name: scratch
          mountPath: /scratch
      resources:                          # optional
        requests:
          memory: 28Gi
          cpu: 6
        limits:
          memory: 28Gi
          cpu: 6
  volumes:                               # optional volume list
    - name: scratch
      source: local
$$;
-- Needed for this: GRANT USAGE ON COMPUTE POOL data_app_workers_compute_pool_<USERNAME> TO ROLE data_app_role;

GRANT MONITOR, OPERATE ON SERVICE data_app_db.app_runtime_schema.job_service_<USERNAME>_job_id_<JOB_ID> TO ROLE data_apps_<USERNAME>_role;  -- can't do this on a settings page because we don't know <JOB_ID> ahead of time
