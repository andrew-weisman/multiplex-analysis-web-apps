# Import relevant libraries.
import streamlit as st
import generate_results
import framework.startup as startup
import framework.manage_sessions as manage_sessions
import framework.inspect_database_tables as inspect_database_tables
import framework.monitor_jobs as monitor_jobs
import framework.platform_abstraction as pa

ST_KEY_PREFIX = "app.py__"
ST_KEY_PREFIX_STARTUP = "startup.py__"


# Define the main function.
def main():

    # Run one-time initialization.
    key = ST_KEY_PREFIX + "app_initialized"
    if key not in st.session_state:
        startup.initialize()
        st.session_state[key] = True

    # Define the pages for the navigation bar.
    pg = st.navigation(
        {
            "Menu": [
                st.Page(manage_sessions.main, title="Manage sessions", default=True, url_path='manage_sessions'),
                st.Page(generate_results.main, title="Generate results", url_path='generate_results'),
                st.Page(monitor_jobs.main, title="Monitor jobs", url_path='monitor_jobs'),
                st.Page(inspect_database_tables.main, title="Inspect database tables", url_path='inspect_database_tables'),
                ],
        }
    )

    # For widget persistence between pages, we need always copy the session state to itself.
    for key in st.session_state:
        if not key.endswith('__do_not_persist'):  # Could add things like: "(not key.endswith('_button'))".
            st.session_state[key] = st.session_state[key]

    # This is needed for the st.dataframe_editor() class (https://github.com/andrew-weisman/streamlit-dataframe-editor) but is also useful for seeing where we are and where we've been.
    st.session_state['current_page_name'] = pg.url_path if pg.url_path != '' else 'Home'
    if 'previous_page_name' not in st.session_state:
        st.session_state['previous_page_name'] = st.session_state['current_page_name']

    # Allow user to shut down entire app cleanly.
    with st.sidebar:
        with st.container(horizontal=True):
            st.button("🔄 Refresh page", help="If you want to refresh the page, press this button, *not* your browser's refresh button.")
            if st.button("🧹 Reset app", help="Reset the app to its initial state."):
                manage_sessions.reset_session_state()
            if st.button("🛑 Shut down app", help="Always save the app session prior to shutdown (unless you don't want to resume your work). Even if you have a running job, you can still shut down the app; just make sure you've saved the app session first so you can pick back up where you left off and load the completed job results as usual."):
                pa.record_explicit_shutdown_time(st.session_state[ST_KEY_PREFIX_STARTUP + "app_session_id"])
                pa.shut_down_app()

    # Write a banner if this app session is waiting on a job to complete.
    if "JOB_PENDING" in st.session_state:
        st.warning(f"This app session is awaiting the results of job {st.session_state['JOB_PENDING']['job_id']}. Please monitor this on the job monitor page.")

    # Display the title of the page.
    st.title(st.session_state[ST_KEY_PREFIX_STARTUP + "app_settings"]['general']['app_title'] + ': ' + pg.title)

    # Display the page.
    pg.run()

    # Update the previous page location.
    st.session_state['previous_page_name'] = st.session_state['current_page_name']


# Run the main function.
if __name__ == "__main__":
    main()
