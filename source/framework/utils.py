import streamlit as st
import uuid
import datetime
import zoneinfo
import pathlib
import shutil
import pickle
import dill
import os
import zipfile
import io
import yaml

ST_KEY_PREFIX_STARTUP = "startup.py__"
SETTINGS_FILENAME = "settings.yaml"


# Not caching since the user could connect to the same Streamlit server by e.g. hitting refresh or opening a new tab at the same URL and would expect a new session directory.
# This function, session_dir(), and jobs_dir() below are the two places in the codebase that hardcode the local container directory to where any files are written in the app. Also, on Snowflake etc. we mount local storage at /tmp/full_stack_data_app, so this is the isolated location where we can modify and understand these settings clearly, i.e., the only places where the app interacts with the local filesystem.
def session_dir():

    if ST_KEY_PREFIX_STARTUP + "app_session_id" not in st.session_state:
        st.error("Session ID not found in session state; cannot return the session directory.")
        return None
    
    app_session_id = st.session_state[ST_KEY_PREFIX_STARTUP + "app_session_id"]
    
    session_dir = f"/tmp/full_stack_data_app/app_session_data/{app_session_id}"

    os.makedirs(session_dir, exist_ok=True)

    return session_dir


@st.cache_data()
def jobs_dir():
    """Root directory for per-job temporary data.
    """
    jobs_dir = "/tmp/full_stack_data_app/job_data"
    os.makedirs(jobs_dir, exist_ok=True)
    return jobs_dir


def get_unique_id():
    return uuid.uuid4().hex


def get_timestamp(timezone="America/New_York"):
    return datetime.datetime.now(zoneinfo.ZoneInfo(timezone))


def ensure_empty_directory(directory, create_if_missing=True):
    """Ensure a directory exists and is empty, creating it if necessary."""
    try:
        directory_path = pathlib.Path(directory)
        if directory_path.exists():
            shutil.rmtree(directory_path)

        if create_if_missing:
            directory_path.mkdir(parents=True, exist_ok=True)

        return True
    except Exception as e:
        st.error(f"Failed to ensure empty directory {directory}: {e}")
        return False


def serialize_dictionary_to_binary_files(dictionary, dict_name, directory, ignore_do_not_persist_flag=True):
    """Save the entire session state efficiently to the session directory"""
    try:
        serializable_dict = {}
        serializable_objects = {}
        unserializable_dict = {}
        unserializable_objects = {}

        for key, value in dictionary.items():
            if ignore_do_not_persist_flag or (not key.endswith("__do_not_persist")):
                try:
                    pickle.dumps(value)  # Test pickling
                    serializable_dict[key] = value
                    serializable_objects[key] = type(value).__name__
                except (TypeError, AttributeError, pickle.PicklingError):
                    unserializable_dict[key] = value
                    unserializable_objects[key] = type(value).__name__

        pkl_file = os.path.join(directory, f'{dict_name}.pkl')
        with open(pkl_file, 'wb') as f:
            f.write(pickle.dumps(serializable_dict))

        dill_file = os.path.join(directory, f'{dict_name}.dill')
        with open(dill_file, 'wb') as f:
            f.write(dill.dumps(unserializable_dict))

        return serializable_objects, unserializable_objects
    except Exception as e:
        st.error(f"Failed to serialize dictionary {dict_name} to directory {directory}: {e}")
        return None


def deserialize_binary_files_to_dictionary(dict_name, directory, dictionary=None):
    try:
        if dictionary is None:
            dictionary = {}

        pkl_file = os.path.join(directory, f'{dict_name}.pkl')
        if os.path.exists(pkl_file):
            with open(pkl_file, 'rb') as f:
                dictionary.update(pickle.loads(f.read()))

        dill_file = os.path.join(directory, f'{dict_name}.dill')
        if os.path.exists(dill_file):
            with open(dill_file, 'rb') as f:
                dictionary.update(dill.loads(f.read()))

        return dictionary
    except Exception as e:
        st.error(f"Failed to deserialize binary files {dict_name}.pkl/.dill to dictionary in directory {directory}: {e}")
        return None


def zip_directory_to_buffer(directory, compresslevel=6):
    """Create a zip archive of a directory"""
    try:
        main_path = pathlib.Path(directory)
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED, compresslevel=compresslevel) as zip_file:
            for file_path in main_path.rglob('*'):
                if file_path.is_file() and not file_path.name.startswith('.'):
                    arcname = file_path.relative_to(main_path)
                    try:
                        zip_file.write(file_path, arcname)
                    except (OSError, IOError) as e:
                        st.warning(f"Skipping file {file_path}: {e}")
        zip_buffer.seek(0)
        return zip_buffer
    except Exception as e:
        st.error(f"Failed to zip directory {directory}: {e}")
        return None


def unzip_buffer_to_directory(zip_buffer, directory):
    try:
        if zip_buffer:
            with zipfile.ZipFile(zip_buffer, 'r') as zip_file:
                zip_file.extractall(directory)
        return True
    except Exception as e:
        st.error(f"Failed to unzip buffer to {directory}: {e}")
        return False


@st.cache_data()
def platform():
    key = ST_KEY_PREFIX_STARTUP + "app_settings"
    if key not in st.session_state:  # When run as a Streamlit app, this will be False as this gets set in startup.py. However, as a worker, startup.py does not get run, so this will be True, so we need to rerun here what startup.py does.
        with open(SETTINGS_FILENAME, 'r') as f:
            st.session_state[key] = yaml.safe_load(f)
    app_platform = os.getenv("APP_PLATFORM")
    if app_platform in ("local", "snowflake"):
        return app_platform
    else:
        st.warning(f"APP_PLATFORM environment variable is not set to a valid value ('local' or 'snowflake'). Detected value: {app_platform}. Falling back to automatic detection.")
    if os.getenv("SNOWFLAKE_ACCOUNT") is not None:
        return "snowflake"
    else:
        return "local"
    # return st.session_state[key]["general"]["platform"]


@st.cache_data()
def app_schema_name():
    return st.session_state[ST_KEY_PREFIX_STARTUP + "app_settings"]["general"]["app_schema_name"]
