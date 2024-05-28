from dataclasses import dataclass
from operator import methodcaller
from typing import ClassVar

import streamlit as st
from snowflake.snowpark.functions import col, max
from snowflake.snowpark.session import Session
from snowflake.snowpark.table import Table


@dataclass
class MyFilter:
    """This dataclass represents the filter that can be optionally enabled.

    It is created to parametrize the creation of filters from Streamlit and to keep the state."""

    # Class variables
    table_name: ClassVar[str]
    session: ClassVar[Session]

    # The name to display in UI
    human_name: str
    # Corresponding column in the table
    table_column: str
    # ID of the streamlit slider
    widget_id: str
    # The type of streamlit widget to generate
    widget_type: callable
    # Field to track if the filter is active. Can be used for filtering
    is_enabled: bool = False
    # max value
    _max_value: int = 0
    # dataframe filter. The name of the dataframe method to be applied as a result of this filter
    _df_method: str = ""

    def __post_init__(self):
        if self.widget_type not in (st.select_slider, st.checkbox, st.selectbox):
            raise NotImplemented

        if self.widget_type is st.select_slider:
            self._df_method = "between"
            self._max_value = (
                self.session.table(self.table_name)
                .select(max(col(self.table_column)))
                .collect()[0][0]
            )

        elif self.widget_type is st.checkbox:
            self._df_method = "__eq__"

        elif self.widget_type is st.selectbox:
            self._df_method = "__eq__"
            self._distinct_value = (
                [ m[0] for m in 
                self.session.table(self.table_name)
                .select(col(self.table_column))
                .distinct()
                .collect()
                ]
            )

    @property
    def max_value(self):
        return self._max_value

    @property
    def distinct_value(self):
        return self._distinct_value

    @property
    def df_method(self):
        return self._df_method

    def get_filter_value(self):
        """Custom unpack function that retrieves the value of the filter
        from session state in a format compatible with self.df_method"""
        _val = st.session_state.get(self.widget_id)
        if self.widget_type is st.checkbox:
            # For .eq
            return dict(other=_val)
        elif self.widget_type is st.select_slider:
            # For .between
            return dict(lower_bound=_val[0], upper_bound=_val[1])
        if self.widget_type is st.selectbox:
            # For .eq
            return dict(other=_val)
        else:
            raise NotImplemented

    def enable(self):
        self.is_enabled = True

    def disable(self):
        self.is_enabled = False

    def create_widget(self):
        if self.widget_type is st.select_slider:
            base_label = "Select the range of"
        elif self.widget_type is st.checkbox:
            base_label = "Is"
        else:
            base_label = "Choose"

        widget_kwargs = dict(label=f"{base_label} {self.widget_id}", key=self.widget_id)

        if self.widget_type is st.select_slider:
            widget_kwargs.update(
                dict(
                    options=list(range(self.max_value + 1)),
                    value=(0, self.max_value),
                )
            )
        elif self.widget_type is st.selectbox:
            widget_kwargs.update(
                dict(
                    options=list(self.distinct_value)
                )
            )

        self.widget_type(**widget_kwargs)

    def __call__(self, _table: Table):
        """This method turns this class into a functor allowing to filter the dataframe.

        This allows to call it:

        f = MyFilter(...)
        new_table = last_table[f(last_table)]"""
        return methodcaller(self.df_method, **(self.get_filter_value()))(
            _table[self.table_column.upper()]
        )

    def __getitem__(self, item):
        return getattr(self, item)
