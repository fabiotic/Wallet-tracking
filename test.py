import pandas as pd
from pycoingecko import CoinGeckoAPI
from dotenv import dotenv_values
import requests
import json
import time
from datetime import datetime

config = dotenv_values(".env")
cg = CoinGeckoAPI()

class chain_:
    def __init__(self, name, symbol, apikey, api):
        self.name = name
        self.symbol = symbol
        self.apikey = apikey
        self.api = api

ETH = chain_('ethereum','ETH', config['ETH'], 'api.etherscan.io')
BSC = chain_('binance-smart-chain', 'BSC', config['BSC'], 'api.bscscan.com')
POLY = chain_('polygon-pos', 'POLY', config['POLY'], 'api.polygonscan.com')
FTM = chain_('fantom', 'FTM', config['FTM'], 'api.ftmscan.com')
AVAX = chain_('avalanche', 'AVAX', config['AVAX'], 'api.snowtrace.io')
CRO = chain_('cronos', 'CRO', config['CRO'], 'api.cronoscan.com')
ARBI = chain_('arbitrum-one', 'ARBI', config['ARBI'], 'api.arbiscan.io')
AURORA = chain_('aurora', 'AURORA', config['AURORA'], 'api.aurorascan.dev')

chains = [ETH,BSC,POLY,FTM,AVAX,CRO,ARBI,AURORA]

cg_rpc_limit = 0
lists = {'address':[], 'hash':[], 'day':[], 'token':[], 'contract address':[], 'flow':[], 'USD':[], 'counterparty':[], 'chain':[]}

def get_txns(chain_,address, start_block, end_block):
    get_request = 'https://{chain}/api?module=account&action=tokentx&address={addy}&startblock={start}&endblock={end}&sort=asc&apikey={api}'.format(chain = chain_.api, addy = address, start = start_block, end = end_block, api = chain_.apikey)
    r = requests.get(get_request)
    return (json.loads(r.text))['result']

def get_start_and_end_block(chain_,start,end):
    start_time = int(time.mktime(time.strptime(start,'%d/%m/%Y')))
    end_time = int(time.mktime(time.strptime(end,'%d/%m/%Y')))
    get_start = 'https://{chain}/api?module=block&action=getblocknobytime&timestamp={time}&closest=before&apikey={api}'.format(chain = chain_.api,time = start_time, api = chain_.apikey)
    get_end = 'https://{chain}/api?module=block&action=getblocknobytime&timestamp={time}&closest=before&apikey={api}'.format(chain = chain_.api,time = end_time, api = chain_.apikey)
    r = requests.get(get_start)
    start_block = (json.loads(r.text))['result']
    r = requests.get(get_end)
    end_block = (json.loads(r.text))['result']
    return(start_block,end_block)

def get_id(chain,token):
    try:
        id = cg.get_coin_info_from_contract_address_by_id(f'{chain}',token)['id']
        return(id)
    except requests.exceptions.HTTPError as e:
        print('waiting')
        time.sleep(70)
        id = get_id(chain,token)
        return(id)
    except ValueError as e:
        return(0)

def get_price(id,date):
    if id == 0:
        return(0)
    else:
        try:
            try:
                price = cg.get_coin_history_by_id(id,date)['market_data']['current_price']['usd']
                return(price)
            except KeyError:
                return 0
        except requests.exceptions.HTTPError as e:
            print('waiting')
            time.sleep(70)
            price = get_price(id,date)
            return(price)

def get_txn_and_list(chain_,address,start,end):
    start_block,end_block = get_start_and_end_block(chain_,start,end)
    get_txn = get_txns(chain_, address, start_block, end_block)
    chain = chain_.name

    for txn in get_txn:
        #append address
        lists['address'].append(address)
        #append hash
        lists['hash'].append(txn['hash'])
        #append day
        utc_time = datetime.utcfromtimestamp(int(txn['timeStamp']))
        time_ = utc_time.strftime("%d-%m-%Y")
        lists['day'].append(time_)
        #append symbol
        tokensymbol = txn['tokenSymbol']
        lists['token'].append(tokensymbol)
        #append contract address
        contract_address = txn['contractAddress']
        lists['contract address'].append(contract_address)
        #append flow
        if txn['to'] == address.lower():
            flow = int(txn['value']) / (10 ** int(txn['tokenDecimal']))
            lists['flow'].append(flow)
        else:
            flow = int(txn['value']) / (-10 ** int(txn['tokenDecimal']))
            lists['flow'].append(flow)
        #append USD
        #first use the contract address to find the id on coingecko
        #then use id to find price on day
        id = get_id(chain,contract_address)
        price = get_price(id,time_)
        lists['USD'].append(price*flow)
        #append counterparty
        if txn['to'] == address.lower():
            lists['counterparty'].append(txn['from'])
        else:
            lists['counterparty'].append(txn['to'])
        #append chain
        lists['chain'].append(chain)
        print('txn added')
    print('all txns added for chain')
    

def get_frame(addresses,start,end):
    for address in addresses:
        for chain_ in chains:
            get_txn_and_list(chain_,address,start,end)
        print('address done')
    print('all address done, creating frame')
    #create dataframe
    df = pd.DataFrame(data=lists,columns=['address', 'hash', 'day', 'token', 'contract address', 'flow', 'USD', 'counterparty', 'chain'])
    df.sort_values('contract address', inplace=True)
    df.to_csv('df.csv')
    df1=df.groupby(['address','token', 'contract address', 'chain'])['flow','USD'].sum()
    df1.reset_index(inplace=True)
    df1.drop(columns='flow')
    print(df1)
    df1.to_csv('df1.csv')
    df2=df.groupby(['address','token', 'contract address', 'chain'])['USD'].sum()
    df3= df2.unstack('chain')
    df4=df3.reset_index()
    col_to_plot = df4.columns.tolist()
    df4[col_to_plot].plot(x='token',kind='bar', stacked=True)
    df4.to_csv('df4.csv')
    df5= df2.unstack('address')
    df6=df5.reset_index()
    col_to_plot = df6.columns.tolist()
    df6[col_to_plot].plot(x='token',kind='bar', stacked=True)
    df6.to_csv('df6.csv')

get_frame(['0x534a0076fb7c2b1f83fa21497429ad7ad3bd7587','0x8e04af7f7c76daa9ab429b1340e0327b5b835748'],'15/06/2022','01/07/2022')