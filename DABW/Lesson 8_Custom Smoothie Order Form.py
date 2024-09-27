# Import python packages
import streamlit as st
from snowflake.snowpark.functions import col
import requests
import pandas as pd

# Write directly to the app
st.title(":cup_with_straw: Customise Your Smoothie! :cup_with_straw:")
st.write(
    """Choose the fruits you want in your custom Smoothie!
    """
)

#Give users a text box to write their name
name_on_order = st.text_input("Name on Smoothie:")
#show them their name
st.write('The name on your Smoothie will be: ',name_on_order)

cnx = st.connection("snowflake")
session = cnx.session()
my_dataframe = session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME'),col('SEARCH_ON'))

# st.dataframe(data=my_dataframe, use_container_width=True)
# st.stop()

#Convert the snowpark Datafram to a pandas dataframe soe we can use the LOC function
pd_df = my_dataframe.to_pandas()
# st.dataframe(pd_df)
# st.stop()


ingredients_list = st.multiselect(
    "Choose up to 5 fruits:"
    , my_dataframe
    , max_selections=5
)


if ingredients_list:
    #show the difference between st.write() and st.text()
    # st.write(ingredients_list)
    # st.text(ingredients_list)

    #Create a blank string to put our ingredients in
    ingredients_string = ''

    #For loop to concatenate each fruit to the string
    for fruit in ingredients_list:
        ingredients_string += fruit +' '

        search_on=pd_df.loc[pd_df['FRUIT_NAME'] == fruit, 'SEARCH_ON'].iloc[0]
        # st.write('The search value for ',fruit,' is ',search_on,'.')


        #add some formatting
        st.subheader(fruit + ' Nutrition Information')
        #get info from fruityvice api
        fruityvice_response = requests.get("https://fruityvice.com/api/fruit/"+search_on)
        #create a data frame with the response
        # fv_df = st.dataframe(data=fruityvice_response.json(), use_container_width=True)
        #parse the json
        pd_fr = fruityvice_response.json()
        #convert to dataframe
        pd_dffr = pd.DataFrame.from_dict(pd_fr)
        #rename the columns
        ren = pd_dffr.rename(columns={"name": "Fruit", "nutritions": "Nutritional Value"})
        #only select 1 column
        out = ren["Nutritional Value"]
        #output
        st.dataframe(out, use_container_width=False)




    #write the selected output to app
    # st.write(ingredients_string)

    #Build a sql insert statement
    my_insert_stmt = """ insert into smoothies.public.orders(ingredients,name_on_order)
values ('""" + ingredients_string + """','"""+name_on_order+"""')"""

    #check the query is right
    #st.write(my_insert_stmt)

    #stop the app when testing
    #st.stop()
    time_to_insert = st.button('Submit Order')

    #if there's anything in the string, then run the insert statement and write the output 
    if time_to_insert:
        session.sql(my_insert_stmt).collect()
        st.success('Your Smoothie is ordered, '+name_on_order+'!', icon="✅")



