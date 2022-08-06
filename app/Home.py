import streamlit as st
from utils import dex_subgraphs_wrapper
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd

@st.cache(allow_output_mutation=True, show_spinner=False)
def st_get_dex_subgraphs_wrapper():
	return dex_subgraphs_wrapper.DexSubgraphsWrapper()

@st.cache(show_spinner=False)
def st_get_swaps_df_from_all_dexes_singlethreaded(list_of_wallets):
     dex_subgraphs_wrapper = st_get_dex_subgraphs_wrapper()
     return dex_subgraphs_wrapper.get_swaps_df(list_of_wallets)

st.set_page_config(
     page_title="Swaps Analyzooor",
     page_icon="🔎",
     layout="wide",
     initial_sidebar_state="collapsed",
     menu_items={
          'About': 'analyzooor.notawizard.xyz'}
 )

st.title("Swaps Analyzooor")
st.markdown('check our landing page: [analyzooor.notawizard.xyz](http://analyzooor.notawizard.xyz/)')

st.info("Fetching the data may take some time. Please be patient. (it usually takes less than 1 minute)")

wallets = st.text_input(
     label="Enter the wallet address here (or multiple addresses, separated by commas)", 
     value="0x5dd596c901987a2b28c38a9c1dfbf86fffc15d77"
)

list_of_wallets = [wallet.strip() for wallet in wallets.split(",")]

should_show_processed_wallets = st.checkbox('Show processed wallets', False)

if should_show_processed_wallets:
     st.write(list_of_wallets)

show_available_dexes = st.checkbox('Show available dexes', False)
if show_available_dexes:
     projects = [
          "apeswap-bsc", "apeswap-polygon",
          "balancer-v2-arbitrum", "balancer-v2-ethereum",
          "balancer-v2-polygon", "bancor-v3-ethereum",
          "beethoven-x-fantom",
          "beethoven-x-optimism", "curve-finance-arbitrum",
          "curve-finance-avalanche", "curve-finance-fantom",
          "curve-finance-gnosis", "curve-finance-ethereum",
          "curve-finance-polygon", "curve-finance-optimism",
          "honeyswap-gnosis", "platypus-avalanche",
          "quickswap-polygon", "saddle-finance-arbitrum",
          "saddle-finance-fantom", "saddle-finance-ethereum",
          "saddle-finance-optimism", "solarbeam-moonriver",
          "spiritswap-fantom", "spookyswap-fantom",
          "sushiswap-arbitrum", "sushiswap-avalanche",
          "sushiswap-bsc", "sushiswap-celo",
          "sushiswap-fantom", "sushiswap-fuse",
          "sushiswap-gnosis", "sushiswap-ethereum",
          "sushiswap-polygon", "sushiswap-moonriver",
          "sushiswap-moonbeam", "trader-joe-avalanche",
          "ubeswap-celo", "uniswap-v2-ethereum",
          "uniswap-v3-arbitrum", "uniswap-v3-ethereum",
          "uniswap-v3-polygon", "uniswap-v3-optimism", 
     ]

     st.write(projects)


types_of_plots = st.multiselect(
     'Select what you want to see', 
     [
          'scatter', 
          'heatmap per symbol',
          'top pools and dexes',
          'net token volume',
          'absolute token volume',
          'raw data',
     ]
)

cols_desc_expander = st.expander("Columns description:")
cols_desc_expander.markdown("""
- swapper: the address of the wallet that swapped the tokens
- swap_datetime: the date and time of the swap
- dex: the name of the decentralized exchange
- token_address_in: token address that was sold on the swap
- token_symbol_in: token symbol that was sold on the swap
- amount_in: amount of tokens sold on the swap
- amount_in_usd: amount of tokens sold on the swap in USD
- token_in_approx_price_usd: approximate price of the token sold on the swap in USD
- token_address_out: token address that was bought on the swap
- token_symbol_out: token symbol that was bought on the swap
- amount_out: amount of tokens bought on the swap
- amount_out_usd: amount of tokens bought on the swap in USD
- token_out_approx_price_usd: approximate price of the token bought on the swap in USD
- conversion_rate_in_to_out: the conversion rate of the token sold on the swap to the token bought on the swap 
- pool_address: the address of the pool that the tokens were sold on
- pool_name: the name of the pool that the tokens were sold on
- tx_hash: the transaction hash of the swap
- log_index: the log index of the swap
- dummy: column with all values as 999 to use as a dummy column
""")

if types_of_plots:
     with st.spinner('Loading data...'):
          swaps_df = st_get_swaps_df_from_all_dexes_singlethreaded(list_of_wallets).copy()
          swaps_df['dummy'] = 999
          dex_data_cols = list(swaps_df.columns)

     if 'scatter' in types_of_plots:
          st.subheader("Scatter plot")
          st.info("The idea here is for you to build your own scatter plot using the features you want")

          scatter_col1, scatter_col2 = st.columns(2)
          
          x_col = scatter_col1.selectbox('Select column for x-axis', dex_data_cols, index=dex_data_cols.index('swap_datetime'))
          y_col = scatter_col2.selectbox('Select column for y-axis', dex_data_cols, index=dex_data_cols.index('amount_out_usd'))
          color_col = scatter_col1.selectbox('Select column for color (can be empty)', dex_data_cols+[None], index=dex_data_cols.index('pool_name'))
          size_col = scatter_col2.selectbox('Select column for size (can be empty)', dex_data_cols+[None], index=dex_data_cols.index('dummy'))
          facet_row_col = scatter_col1.selectbox('Select column for facet row (can be empty)', dex_data_cols+[None], index=len(dex_data_cols))
          
          show_boxplot = scatter_col2.selectbox('Show boxplot', [True, False], index=0)
          plot_height = scatter_col1.number_input('Height of the plot', value=600, min_value=100, max_value=6_000)
          # plot_width = scatter_col2.number_input('Width of the plot', value=1800, min_value=100, max_value=10_000)
          dot_opacity = scatter_col2.number_input('Select opacity', value=0.5, step=0.1, min_value=0.1, max_value=1.0)
          dot_max_size = scatter_col1.number_input('Select max size', value=10, step=1, min_value=1)
          plot_title = scatter_col2.text_input('Title', value='dex scatter plot')

          fig = px.scatter(
               swaps_df, x=x_col, y=y_col, color=color_col, size=size_col, title=plot_title, height=plot_height, # width=plot_width,
               opacity=dot_opacity, size_max=dot_max_size, marginal_y='box' if show_boxplot else None, facet_row=facet_row_col,
               hover_data=['swap_datetime', 'dex', 'token_symbol_in', 'amount_in', 'amount_in_usd', 'token_in_approx_price_usd', 
               'token_symbol_out', 'amount_out', 'amount_out_usd', 'token_out_approx_price_usd', 'conversion_rate_in_to_out', 'pool_name'])

          st.plotly_chart(fig, use_container_width=True)

     
     if 'heatmap per symbol' in types_of_plots:
          st.subheader("Heatmap per symbol")
          st.info("The idea here is to have an easy way to identify popular swap pairs")
          
          heatmap_metric = st.selectbox('Select a metric for the heatmap', ['usd volume', 'number of swaps'])
          heatmap_show_top_n = st.number_input('Show top n swap pairs', value=30, step=1, min_value=1)

          if heatmap_metric=='usd volume':
               agg_tokens_by_volume = swaps_df.groupby(['token_symbol_in','token_symbol_out'])['amount_in_usd'].sum().sort_values(ascending=False).reset_index().head(heatmap_show_top_n)
          elif heatmap_metric=='number of swaps':
               agg_tokens_by_volume = swaps_df.groupby(['token_symbol_in','token_symbol_out'])['amount_in_usd'].count().sort_values(ascending=False).reset_index().head(heatmap_show_top_n)

          pivoted_tokens_by_volumes = pd.pivot(agg_tokens_by_volume, index='token_symbol_in', columns='token_symbol_out', values='amount_in_usd')

          fig = px.imshow(pivoted_tokens_by_volumes, text_auto=True, title=f'Heatmap of the {heatmap_metric} by token in and out', aspect='equal', height=1000, width=1000)
          fig.update_xaxes(nticks=100, tickfont={'size': 10}, showgrid=False)
          fig.update_yaxes(nticks=100, tickfont={'size': 10}, showgrid=False)
          fig.update_layout(title=f'Top {heatmap_show_top_n} Transfers Heatmap')
          st.plotly_chart(fig, use_container_width=True)

     if 'top pools and dexes' in types_of_plots:
          st.subheader("Top pools and dexes")
          st.info("This should give you a good idea of the most popular liquidity pools and dexes")

          top_pools_metric = st.selectbox('Select a metric for the top pools', ['usd volume', 'number of swaps'])
          font_size = st.number_input('Sunburst Font size', value=14, step=1, min_value=1)

          if top_pools_metric=='usd volume':
               agg_dex_and_pools = swaps_df.groupby(['dex', 'pool_name'])['amount_in_usd'].sum().reset_index()
          elif top_pools_metric=='number of swaps':
               agg_dex_and_pools = swaps_df.groupby(['dex', 'pool_name'])['amount_in_usd'].count().reset_index()

          fig = px.sunburst(agg_dex_and_pools[agg_dex_and_pools['amount_in_usd']!=0], path=['dex', 'pool_name'], values='amount_in_usd', height=800)
          fig.update_layout(
               title=f'Top Pools and DEXes by {top_pools_metric}',
          font=dict(
               size=font_size,
          )
          )
          st.plotly_chart(fig, use_container_width=True)

          weekly_dex_vol = swaps_df.set_index('swap_datetime').groupby([pd.Grouper(freq='7d'), 'dex'])['amount_in_usd'].sum().reset_index()

          fig = px.bar(weekly_dex_vol, x='swap_datetime', y='amount_in_usd', title='Weekly Volume in USD per DEX', height=800, color='dex')
          st.plotly_chart(fig, use_container_width=True)

          weekly_dex_vol = swaps_df.set_index('swap_datetime').groupby([pd.Grouper(freq='7d'), 'pool_name'])['amount_in_usd'].sum().reset_index()

          fig = px.bar(weekly_dex_vol, x='swap_datetime', y='amount_in_usd', title='Weekly Volume in USD per Pool', height=800, color='pool_name')
          st.plotly_chart(fig, use_container_width=True)

     if 'net token volume' in types_of_plots:
          st.subheader("Net token volume")
          st.info("This should give you a good idea of the trading volume across tokens. Net volumme = Tokens swapped to - Tokens swapped from")

          net_token_volume_show_top_n = st.number_input('Show top n swap pairs for net token volume', value=15, step=1, min_value=1)

          outflows = swaps_df[['swapper', 'dex', 'swap_datetime', 'token_symbol_in', 'amount_in_usd', 'pool_name']]
          outflows['amount_in_usd'] = -outflows['amount_in_usd']
          outflows.columns=['swapper', 'dex', 'swap_datetime', 'token_symbol', 'amount_usd', 'pool_name']

          inflows = swaps_df[['swapper', 'dex', 'swap_datetime', 'token_symbol_out', 'amount_out_usd', 'pool_name']]
          inflows.columns=['swapper', 'dex', 'swap_datetime', 'token_symbol', 'amount_usd', 'pool_name']

          movements = pd.concat([outflows, inflows])

          total_netted = movements.groupby(['token_symbol'])['amount_usd'].sum().reset_index()
          total_netted = total_netted[total_netted['amount_usd']!=0]
          total_netted = total_netted[total_netted['token_symbol']!='']
          total_netted = total_netted.sort_values(by='amount_usd', ascending=False)

          top_netted = total_netted.head(net_token_volume_show_top_n)
          fig = px.bar(top_netted, x='token_symbol', y='amount_usd', title=f'Top {net_token_volume_show_top_n} Tokens by Netted Volume in USD', height=800, text_auto=True)
          st.plotly_chart(fig, use_container_width=True)

          bottom_netted = total_netted.tail(net_token_volume_show_top_n)
          fig = px.bar(bottom_netted, x='token_symbol', y='amount_usd', title=f'Bottom {net_token_volume_show_top_n} Tokens by Netted Volume in USD', height=800, text_auto=True)
          st.plotly_chart(fig, use_container_width=True)

          weekly_plot_col = 'token_symbol' 
          weekly_movement = movements.set_index('swap_datetime').groupby([pd.Grouper(freq='7d'), weekly_plot_col])['amount_usd'].sum().reset_index()
          weekly_movement = weekly_movement[weekly_movement['amount_usd']!=0]
          weekly_movement = weekly_movement[weekly_movement[weekly_plot_col]!='']
          weekly_movement = weekly_movement.sort_values(by='amount_usd', ascending=False)

          fig = px.bar(weekly_movement, x='swap_datetime', y='amount_usd', title='Weekly Netted Volume in USD per token', height=800, color=weekly_plot_col)
          st.plotly_chart(fig, use_container_width=True)


     if 'absolute token volume' in types_of_plots:
          st.subheader("Absolute token volume")
          st.info("This should give you a good idea of the trading volume across tokens. Absolute volume = Tokens swapped to + Tokens swapped from") 

          abs_token_volume_show_top_n = st.number_input('Show top n swap pairs for absolute token volume', value=15, step=1, min_value=1)

          outflows = swaps_df[['swapper', 'dex', 'swap_datetime', 'token_symbol_in', 'amount_in_usd', 'pool_name']]
          outflows['amount_in_usd'] = outflows['amount_in_usd']
          outflows.columns=['swapper', 'dex', 'swap_datetime', 'token_symbol', 'amount_usd', 'pool_name']

          inflows = swaps_df[['swapper', 'dex', 'swap_datetime', 'token_symbol_out', 'amount_out_usd', 'pool_name']]
          inflows.columns=['swapper', 'dex', 'swap_datetime', 'token_symbol', 'amount_usd', 'pool_name']

          abs_movements = pd.concat([outflows, inflows])

          total_abs = abs_movements.groupby(['token_symbol', 'dex'])['amount_usd'].sum().reset_index()
          total_abs = total_abs[total_abs['amount_usd']!=0]
          total_abs = total_abs[total_abs['token_symbol']!='']
          total_abs = total_abs.sort_values(by='amount_usd', ascending=False)

          fig = px.bar(total_abs.head(abs_token_volume_show_top_n), x='token_symbol', y='amount_usd', title='Total Absolute Volume in USD per token', height=800, text_auto=True, color='dex')
          fig.update_layout(barmode='stack', xaxis={'categoryorder':'total descending'})    
          st.plotly_chart(fig, use_container_width=True)

          weekly_plot_col = 'token_symbol' 
          weekly_movement = abs_movements.set_index('swap_datetime').groupby([pd.Grouper(freq='7d'), weekly_plot_col])['amount_usd'].sum().reset_index()
          weekly_movement = weekly_movement[weekly_movement['amount_usd']!=0]
          weekly_movement = weekly_movement[weekly_movement[weekly_plot_col]!='']
          weekly_movement = weekly_movement.sort_values(by='amount_usd', ascending=False)

          fig = px.bar(weekly_movement, x='swap_datetime', y='amount_usd', title='Weekly Absolute Volume in USD per token', height=800, color=weekly_plot_col)
          st.plotly_chart(fig, use_container_width=True)

     if 'raw data' in types_of_plots:
          st.subheader("Raw data")
          st.write(swaps_df)
          # download button for dex_data
          dex_data_json = swaps_df.to_json().encode('utf-8')

          #
          csv_data_json = swaps_df.to_csv().encode('utf-8')

          st.download_button(
               label = 'Download json with dex data',
               data=dex_data_json,
               file_name='dex_data.json',
               mime='application/json'
          )

          st.download_button(
               label = 'Download csv with dex data',
               data=csv_data_json,
               file_name='dex_data.csv',
               mime='text/csv'
          )
