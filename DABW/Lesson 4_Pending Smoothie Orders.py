# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col

# Write directly to the app
st.title(":cup_with_straw: Pending Smoothie Orders :cup_with_straw:")
st.write(
    """Orders that need to be filled."""
)

# Get the current credentials
session = get_active_session()

#get the orders to fill and filter out filled orders
orders_to_fill = session.table("smoothies.public.orders").filter(col("ORDER_FILLED")==0).collect()

#let us check the boxes
editable_df = st.data_editor(orders_to_fill)

#add submit button
submitted = st.button('Submit')

if submitted:
    st.success('Someone clicked the button.', icon= "👍")

