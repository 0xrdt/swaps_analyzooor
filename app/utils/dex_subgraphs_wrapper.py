from typing import Tuple, Dict
from subgrounds.subgrounds import Subgrounds
from subgrounds.subgraph.subgraph import Subgraph
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures

pd.options.mode.chained_assignment = None

class DexSubgraphsWrapper():
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

	def __init__(self, threaded=True) -> None:
		self._load_subgraphs(threaded)
		self.row_limit_per_dex = 10_000
		self.threaded = threaded
		

	def _load_subgraphs(self, threaded=True) -> Tuple[Subgrounds, Dict[str, Subgraph]]:
		
		sg = Subgrounds()

		subgraphs = {}

		if not threaded:
			for project in self.projects:
				try: 
					subgraphs[project] = sg.load_subgraph(f"https://api.thegraph.com/subgraphs/name/messari/{project}")
				except Exception as e:
					print(f"Error loading subgraph for {project}: {e}")
					continue
		
		else:		
			with ThreadPoolExecutor() as executor:
				futures = {
					executor.submit(sg.load_subgraph, f"https://api.thegraph.com/subgraphs/name/messari/{project}"): 
					project for project in self.projects
				}
				for future in concurrent.futures.as_completed(futures):
					project = futures[future]
					try:
						subgraphs[project] = future.result()
					except Exception as e:
						print(f"Error loading {project} subgraph: {e}")
						continue

		self.sg = sg
		self.subgraphs = subgraphs

	def _get_swaps_df_from_specific_dex(self, dex_subgraph: Subgraph, where: str) -> pd.DataFrame:

		swaps = dex_subgraph.Query.swaps(first=self.row_limit_per_dex, orderBy=dex_subgraph.Swap.timestamp, orderDirection="desc", where=where)

		data = [
			swaps.timestamp,
			swaps.to,
			swaps.__getattribute__("from"),
			swaps.tokenIn.id,
			swaps.tokenIn.symbol,
			swaps.tokenIn.decimals,
			swaps.amountIn,
			swaps.amountInUSD,
			swaps.tokenOut.id,
			swaps.tokenOut.symbol,
			swaps.tokenOut.decimals,
			swaps.amountOut,
			swaps.amountOutUSD,
			swaps.pool.id,
			swaps.pool.name,
			swaps.pool.symbol,
			swaps.hash,
			swaps.logIndex
		]

		df = self.sg.query_df(data)

		# df['swaps_datetime'] = pd.to_datetime(df['swaps_timestamp'], unit='s')

		return df


	def _get_swaps_df_from_all_dexes_singlethreaded(self, where: str) -> pd.DataFrame:
		list_of_dfs = []
		
		for project, subgraph in self.subgraphs.items():
			print(project)

			try:
				df = self.get_swaps_df_from_specific_dex(subgraph, where)

			except Exception as e:
				print(f"Error processing {project}: {e}")
				continue

			if len(df) > 0:
				df['project'] = project
				list_of_dfs.append(df)

		return pd.concat(list_of_dfs, ignore_index=True)


	def _get_swaps_df_from_all_dexes_multithreaded(self, where: str) -> pd.DataFrame:
		list_of_dfs = []
		with ThreadPoolExecutor(max_workers=4) as executor:
			futures = {executor.submit(self._get_swaps_df_from_specific_dex, subgraph, where): project for project, subgraph in self.subgraphs.items()}
			for future in concurrent.futures.as_completed(futures):
				project = futures[future]
				try:
					df = future.result()
				except Exception as e:
					# print(f"Error processing {project}: {e}")
					print(f"Error processing {project}")
					continue

				if len(df) > 0:
					df['project'] = project
					list_of_dfs.append(df)

		if len(list_of_dfs)>0:
			return pd.concat(list_of_dfs, ignore_index=True)
		else:
			return pd.DataFrame(columns=[
				'swaps_timestamp', 'swaps_to', 'swaps_from', 'swaps_tokenIn_id',
				'swaps_tokenIn_symbol', 'swaps_tokenIn_decimals', 'swaps_amountIn',
				'swaps_amountInUSD', 'swaps_tokenOut_id', 'swaps_tokenOut_symbol',
				'swaps_tokenOut_decimals', 'swaps_amountOut', 'swaps_amountOutUSD',
				'swaps_pool_id', 'swaps_pool_name', 'swaps_pool_symbol', 'swaps_hash',
				'swaps_logIndex', 'project'
			])


	@staticmethod
	def build_clean_df(raw_df: pd.DataFrame) -> pd.DataFrame:
		clean_df = raw_df[['swaps_to']]
		clean_df = clean_df.rename(columns={'swaps_to': 'swapper'})

		clean_df['swap_datetime'] = pd.to_datetime(raw_df['swaps_timestamp'], unit='s')

		clean_df['dex'] = raw_df['project']

		clean_df['token_address_in'] = raw_df['swaps_tokenIn_id']
		clean_df['token_symbol_in'] = raw_df['swaps_tokenIn_symbol']
		clean_df['amount_in'] = raw_df['swaps_amountIn']/10**raw_df['swaps_tokenIn_decimals']
		clean_df['amount_in_usd'] = raw_df['swaps_amountInUSD']
		clean_df['token_in_approx_price_usd'] = clean_df.apply(lambda x: x['amount_in_usd']/x['amount_in'] if x['amount_in']!=0 else 0, axis=1)
		

		clean_df['token_address_out'] = raw_df['swaps_tokenOut_id']
		clean_df['token_symbol_out'] = raw_df['swaps_tokenOut_symbol']
		clean_df['amount_out'] = raw_df['swaps_amountOut']/10**raw_df['swaps_tokenOut_decimals']
		clean_df['amount_out_usd'] = raw_df['swaps_amountOutUSD']
		clean_df['token_out_approx_price_usd'] = clean_df.apply(lambda x: x['amount_out_usd']/x['amount_out'] if x['amount_out']!=0 else 0, axis=1)

		clean_df['conversion_rate_in_to_out'] = clean_df.apply(lambda x: x['amount_in']/x['amount_out'] if x['amount_out']!=0 else 0, axis=1)

		clean_df['pool_address'] = raw_df['swaps_pool_id']
		clean_df['pool_name'] = raw_df['swaps_pool_name']

		clean_df['tx_hash'] = raw_df['swaps_hash']
		clean_df['log_index'] = raw_df['swaps_logIndex']

		return clean_df

	def get_swaps_df(self, wallet_addresses) -> pd.DataFrame:

		# filter by who is swapping
		where = {"to_in": [wallet_address.lower() for wallet_address in wallet_addresses]}

		if self.threaded:
			raw_df = self._get_swaps_df_from_all_dexes_multithreaded(where)
		else:
			raw_df = self._get_swaps_df_from_all_dexes_singlethreaded(where)
		
		clean_df = self.build_clean_df(raw_df)
		
		return clean_df