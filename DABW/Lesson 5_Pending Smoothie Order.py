# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, when_matched

# Write directly to the app
st.title(":cup_with_straw: Pending Smoothie Orders :cup_with_straw:")
st.write(
    """Orders that need to be filled."""
)

# Get the current credentials
session = get_active_session()

#get the orders to fill and filter out filled orders
orders_to_fill = session.table("smoothies.public.orders").filter(col("ORDER_FILLED")==0).collect()

#only show if there are orders
if orders_to_fill:

    #let us check the boxes
    editable_df = st.data_editor(orders_to_fill)
    
    
    
    #add submit button
    submitted = st.button('Submit')
    
    if submitted:
    
        #pull in the original data
        og_dataset = session.table("smoothies.public.orders")
    
        #create a df to merge
        edited_dataset = session.create_dataframe(editable_df)
    
        try:
            #merge the dfs so the order_filled is updated
            og_dataset.merge(edited_dataset
                               , (og_dataset['ORDER_UID'] == edited_dataset['ORDER_UID'])
                               , [when_matched().update({'ORDER_FILLED': edited_dataset['ORDER_FILLED']})]
                              )
        
            #only print if it worked
            st.success('Order(s) Updated!', icon= "👍")
        except:
            #if it failed
            st.write('Something went wrong.')

else:
    st.success('There are no pending orders right now', icon="👍")
