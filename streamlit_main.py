#%% Import python packages
import streamlit as st
import sys, os, inspect, time, platform
from datetime import datetime
from snowflake.snowpark.context import get_active_session
import snowflake.snowpark as sp
import pandas as pd
from typing import Iterable

import plotly.graph_objects as go
from snowflake.snowpark.session import Session
from snowflake.snowpark.table import Table
from toolz.itertoolz import pluck

from libs.utils import get_project_root
project_root = get_project_root()
sys.path.insert(0, f'{project_root}/libs')

from libs.chart_helpers import mk_labels, mk_links
from libs.filterwidget import MyFilter
from libs.compare_medata import CompareMetadata
from libs.raven_app import RavenTargetDB
from libs.build_raven import BuildRaven

#%%
try:
    # Set the page layout to wide
    st.set_page_config(
        layout="wide",
    )
except:
    pass
   
disclaimer = """Disclaimer: Use at your own discretion. This site does not store your Snowflake credentials and your credentials are only used as a passthrough to connect to your Snowflake account."""

def main():
    pass

def _get_active_filters() -> filter:
    return filter(lambda _: _.is_enabled, st.session_state.filters)


def _is_any_filter_enabled() -> bool:
    return any(pluck("is_enabled", st.session_state.filters))


def _get_human_filter_names(_iter: Iterable) -> Iterable:
    return pluck("human_name", _iter)


# Initialize connection.
def init_connection(database_name, app_name) -> Session:
    try:
        return get_active_session()
    except:
        return RavenTargetDB(database_name,app_name).get_snowflake_session()

@st.cache_resource 
#st.cache
def convert_df(df: pd.DataFrame):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode("utf-8")


def draw_sidebar():
    """Should include dynamically generated filters"""

    with st.sidebar:
        selected_filters = st.multiselect(
            "Select which filters to enable",
            list(_get_human_filter_names(st.session_state.filters)),
            [],
        )
        for _f in st.session_state.filters:
            if _f.human_name in selected_filters:
                _f.enable()

        if _is_any_filter_enabled():
            with st.form(key="input_form"):

                for _f in _get_active_filters():
                    _f.create_widget()
                st.session_state.clicked = st.form_submit_button(label="Submit")
        else:
            st.write("Please enable a filter")


def draw_main_ui(_session: Session, _table_name: str):
    """Contains the logic and the presentation of main section of UI"""
    if _is_any_filter_enabled():

        metadata: Table = _session.table(_table_name)
        table_sequence = [metadata]

        _f: MyFilter
        for _f in _get_active_filters():
            # This block generates the sequence of dataframes as continually applying AND filter set by the sidebar
            # The dataframes are to be used in the Sankey chart.

            # First, get the last dataframe in the list
            last_table = table_sequence[-1]
            # Apply the current filter to it
            new_table = last_table[
                # At this point the filter will be dynamically applied to the dataframe using the API from MyFilter
                _f(last_table)
            ]
            table_sequence += [new_table]
        st.header("Dataframe preview")

        st.write(table_sequence[-1].sample(n=500).to_pandas().head(500))

        # Generate the Sankey chart
        fig = go.Figure(
            data=[
                go.Sankey(
                    node=dict(
                        pad=15,
                        thickness=20,
                        line=dict(color="black", width=0.5),
                        label=mk_labels(_get_human_filter_names(_get_active_filters())),
                    ),
                    link=mk_links(table_sequence),
                )
            ]
        )
        st.header("Sankey chart of the applied filters")
        st.plotly_chart(fig, use_container_width=True)

        # Add the SQL statement sequence table
        statement_sequence = """
| number | filter name | query, transformation |
| ------ | ----------- | --------------------- |"""
        st.header("Statement sequence")
        for number, (_label, _table) in enumerate(
            zip(
                mk_labels(_get_human_filter_names(_get_active_filters())),
                table_sequence,
            )
        ):
            statement_sequence += f"""\n| {number+1} | {_label} | ```{_table.queries['queries'][0]}``` |"""

        st.markdown(statement_sequence)

        # Materialize the result <=> the button was clicked
        if st.session_state.clicked:
            with st.spinner("Converting results..."):
                st.download_button(
                    label="Download as CSV",
                    data=convert_df(table_sequence[-1].to_pandas()),
                    file_name="metadata.csv",
                    mime="text/csv",
                )
    else:
        st.write("Please enable a filter in the sidebar to show transformations")

def compare_medata():
    st.header("Dataframe preview")

def call_build(database_name, app_name, chk_metadata, chk_build, chk_grant):
    build = BuildRaven(database_name, app_name, chk_metadata, chk_build, chk_grant, env_number)
    return build.build_raven()


def list_file(solution_path, path):
    
    raven_path = f"{solution_path}/{path}"

    filelist=[]
    for root, dirs, files in os.walk(raven_path):
        for file in files:
                path_to_file=os.path.join(root, file)
                mtime = creation_date(path_to_file)
                last_modified = datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M:%S')
                filelist.append(path_to_file.replace(f"{solution_path}/", "") + " | " + last_modified)
    return filelist


def creation_date(path_to_file):
    """
    Try to get the date that a file last modified.
    """
    if platform.system() == 'Windows':
        return os.path.getmtime(path_to_file)
    else:
        stat = os.stat(path_to_file)
        try:
            return stat.st_birthtime
        except AttributeError:
            # We're probably on Linux. No easy way to get creation dates here,
            # so we'll settle for when its content was last modified
            return stat.st_mtime

def get_snowflake_apps():
    return ('','Musdw', 'Raptor', 'RegOps')

if __name__ == "__main__":
    try:
        overall_config_table = "RAVEN.VW_METADATA_OVERALL_CONFIG"
        root = get_project_root()

        st.title("Raven Embedded")
        options = ["Deploy DVLP", "Metadata Explorer", "Metadata Compare"]
        choice = st.sidebar.selectbox("Menu", options)

        # Set up general laytout
        if choice == "Deploy DVLP":
            st.subheader(choice)
            settings = st.columns(2)
            clear = st.columns(2)
            aggcharts = st.columns(2)
            container = st.container()

            with settings[0]:
                with st.form(key="form"):
                    app_name = st.selectbox('Application Name',get_snowflake_apps())
                    database_name = st.text_input("Database Name")
                    env_number = st.number_input(label="Environment Number", min_value=1, max_value=5, value=1)
                    submit = st.form_submit_button()

            with settings[1]:
                st.text("Deploy Options")
                chk_metadata = bool(st.checkbox("Update Metadata", True, "metadata"))
                chk_build = bool(st.checkbox("Build Raven", True, "build_raven"))
                chk_grant = True if chk_build else False #bool(st.checkbox("Grant permssion on role RW", False, "grant"))
 
            if app_name :
                metadata_list = list_file(root,f"metadata\\seeds\\{app_name}")
                st.subheader("Metadata Files")
                st.write(metadata_list)

                customn_list = list_file(root, f"models\\custom\\{app_name}")
                st.subheader("Custom Files")
                st.write(customn_list)        

            if submit and (chk_metadata or chk_build or chk_grant):
                results = call_build(database_name, app_name, chk_metadata, chk_build, chk_grant)
                st.subheader("Steps Executed")
                for i, result in enumerate(results):
                    st.write(str(i + 1) + '. ' + str(result).replace('{', '').replace('}', '').replace('\'', '\"'))


        elif choice == "Metadata Explorer":
            st.subheader(choice)
            settings = st.columns(4)
            clear = st.columns(4)
            aggcharts = st.columns(4)
            container = st.container()

            with settings[0]:
                app_name = st.selectbox(
                'Application Name',
                get_snowflake_apps())

            with settings[1]:
                database_name = st.text_input("Database Name")

            if (database_name and app_name):
                # Initialize the filters
                session = init_connection(database_name,app_name)
                MyFilter.session = session
                MyFilter.table_name = overall_config_table

                st.session_state.filters = (
                    
                    MyFilter(
                        human_name="Metadata Enable",
                        table_column="STAGE_ME_PARAMETERS_IS_ENABLED",
                        widget_id="Enabled",
                        widget_type=st.checkbox,
                    ),
                    MyFilter(
                        human_name="Flag Trigger File",
                        table_column="IS_TRIGGER_FILE",
                        widget_id="Trigger File",
                        widget_type=st.checkbox,
                    ),
                    MyFilter(
                        human_name="Flag Cloud Copy",
                        table_column="IS_CLOUD_COPY",
                        widget_id="Cloud Copy",
                        widget_type=st.checkbox,
                    ),
                    MyFilter(
                        human_name="Source System Code",
                        table_column="SOURCE_SYSTEM_CODE",
                        widget_id="Source System Code",
                        widget_type=st.selectbox,
                    ),
                    MyFilter(
                        human_name="Source Field Name",
                        table_column="SOURCE_FIELD_NAME",
                        widget_id="Source Field Name",
                        widget_type=st.selectbox,
                    ),
                    MyFilter(
                        human_name="Target Table",
                        table_column="DESTINATION_FULL_TABLE_NAME",
                        widget_id="Target Table",
                        widget_type=st.selectbox,
                    ),
                    MyFilter(
                        human_name="Container",
                        table_column="CONTAINER_NAME",
                        widget_id="Container",
                        widget_type=st.selectbox,
                    ),
                )

                draw_sidebar()
                draw_main_ui(session,overall_config_table)
                
        elif choice == "Metadata Compare":
            st.subheader(choice)
            settings = st.columns(3)
            clear = st.columns(3)
            aggcharts = st.columns(3)
            container = st.container()

            with settings[0]:
                app_name = st.selectbox(
                'Application Name',
                get_snowflake_apps())

            with settings[1]:
                database_name_base = st.text_input("Database Name Base")

            with settings[2]:
                database_name_target = st.text_input("Database Name Target")
   

            if app_name and database_name_base and database_name_target:
                session = init_connection(database_name_base,app_name)
                compare = CompareMetadata(database_name_base,database_name_target)
                st.button("Refresh", key="refresh",)
                
                metadata_dict = {
                    "RAVEN.METADATA_SOURCE_FILE":
                        ['CURRENT_DATABASE_NAME','SOURCE_SYSTEM_CODE','SOURCE_FEED_CODE'],
                    "RAVEN.METADATA_SOURCE_FIELD": 
                        ['CURRENT_DATABASE_NAME','SOURCE_SYSTEM_CODE','SOURCE_FEED_CODE','SOURCE_FIELD_NAME'],
                    "RAVEN.METADATA_STAGE_ME_PARAMETERS": 
                        ['CURRENT_DATABASE_NAME','DATASET_NAME']}

                count_metadata = 0
                for table in metadata_dict:
                    df_diff_summary,df_compare = compare.compare_metadata(table, session)

                    if not df_diff_summary.empty:
                        st.subheader(table)
                        st.write(df_diff_summary.sort_values(by=metadata_dict[table]))
                        st.write(df_compare)
                        count_metadata +=1

                if count_metadata == 0:
                    st.text("Metadata is the same")

        st.write("")
        footer = st.container()
        with footer:
            st.caption(f"""©2023 by Raptor Team""")
            st.caption(f"Streamlit Version: {st.__version__}")
            st.caption(f"Snowpark Version: {sp.__version__}")
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        st.warning(f'{exc_type} - {exc_value} - {exc_traceback}')
        pass
        st.write(f'{exc_type} - {exc_value} - {exc_traceback}')
