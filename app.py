import streamlit as st
from binance import ThreadedWebsocketManager, Client
import pandas as pd
import time
from datetime import datetime
import queue 
import functools 

# --- IMPORTANT NOTE ON PYTHON VERSION ---
# If you are seeing errors like "missing ScriptRunContext" or
# "st.session_state has no attribute 'price_data'" when processing
# messages from a separate thread, it's highly likely you are using
# an unsupported Python version (e.g., Python 3.13, which is pre-release).
# Please ensure you are using a stable Python release like 3.9, 3.10, 3.11, or 3.12.
# --- END IMPORTANT NOTE ---

# --- Configuration ---
BINANCE_API_KEY = ""
BINANCE_API_SECRET = ""

# --- Data Storage and WebSocket Management ---
if 'price_data' not in st.session_state:
    st.session_state.price_data = {} 
if 'websocket_manager' not in st.session_state:
    st.session_state.websocket_manager = None
if 'is_websocket_running' not in st.session_state:
    st.session_state.is_websocket_running = False
if 'binance_client' not in st.session_state:
    st.session_state.binance_client = None
if 'message_queue' not in st.session_state:
    st.session_state.message_queue = queue.Queue()


# --- Callback function for WebSocket messages (puts data into queue) ---
def handle_socket_message(msg, queue_instance): 
    """
    Receives incoming WebSocket messages and puts them into a thread-safe queue.
    This runs in a separate thread and DOES NOT directly access st.session_state.
    """
    # Debugging: print the raw message to confirm it's received by the callback
    # print(f"--- [WS Thread] Received Raw Message: {msg}") 
    queue_instance.put(msg) 


# --- Data Processing Function (runs in main Streamlit thread) ---
def process_messages_from_queue():
    """
    Reads messages from the queue and updates st.session_state.price_data.
    This function should be called from the main Streamlit thread.
    """
    if 'price_data' not in st.session_state:
        st.session_state.price_data = {}
        # print("--- [Main Thread] WARNING: price_data was missing in session_state, re-initializing.")

    while not st.session_state.message_queue.empty():
        try:
            msg = st.session_state.message_queue.get_nowait() # Get message without blocking
            # Debugging: print message being processed by main thread
            # print(f"--- [Main Thread] Processing Message: {msg}")

            if 'e' not in msg:
                # print(f"--- [Main Thread] Skipping message without 'e' key: {msg}")
                continue

            if msg['e'] == '24hrMiniTicker': 
                try:
                    symbol = msg['s']
                    last_price = float(msg['c'])
                    open_price = float(msg['o'])
                    high_price = float(msg['h'])
                    low_price = float(msg['l'])
                    volume = float(msg['v'])
                    quote_volume = float(msg['q'])
                    
                    change_percent = ((last_price - open_price) / open_price) * 100 if open_price != 0 else 0
                    
                    st.session_state.price_data[symbol] = {
                        'Last Price': f"{last_price:,.4f}",
                        '24hr Change %': f"{change_percent:,.2f}%",
                        '24hr High': f"{high_price:,.4f}",
                        '24hr Low': f"{low_price:,.4f}",
                        '24hr Volume (Base)': f"{volume:,.2f}",
                        '24hr Volume (Quote)': f"{quote_volume:,.2f}",
                        'Last Updated': datetime.now().strftime("%H:%M:%S")
                    }
                    # print(f"--- [Main Thread] SUCCESS: Updated data for {symbol}: {st.session_state.price_data[symbol]}")
                except KeyError as e:
                    print(f"--- [Main Thread] ERROR: Missing expected key in 24hrMiniTicker message: {e} in {msg}")
                except ValueError as e:
                    print(f"--- [Main Thread] ERROR: Data conversion issue in 24hrMiniTicker message: {e} for message: {msg}")
                except Exception as e:
                    print(f"--- [Main Thread] UNKNOWN ERROR during 24hrMiniTicker processing: {e} for message: {msg}")

            elif msg['e'] == '24hrTicker': 
                if 'data' in msg and isinstance(msg['data'], list):
                    for ticker in msg['data']:
                        try:
                            symbol = ticker['s']
                            last_price = float(ticker['c'])
                            change_percent = float(ticker['P']) 
                            high_price = float(ticker['h'])
                            low_price = float(ticker['l'])
                            volume = float(ticker['v'])
                            quote_volume = float(ticker['q'])

                            st.session_state.price_data[symbol] = {
                                'Last Price': f"{last_price:,.4f}",
                                '24hr Change %': f"{change_percent:,.2f}%",
                                '24hr High': f"{high_price:,.4f}",
                                '24hr Low': f"{low_price:,.4f}",
                                '24hr Volume (Base)': f"{volume:,.2f}",
                                '24hr Volume (Quote)': f"{quote_volume:,.2f}",
                                'Last Updated': datetime.now().strftime("%H:%M:%S")
                            }
                        except KeyError as e:
                            print(f"--- [Main Thread] ERROR: Missing expected key in 24hrTicker item: {e} in {ticker}")
                        except ValueError as e:
                            print(f"--- [Main Thread] ERROR: Data conversion issue in 24hrTicker item: {e} for item: {ticker}")
                        except Exception as e:
                            print(f"--- [Main Thread] UNKNOWN ERROR during 24hrTicker item processing: {e} for item: {ticker}")
                else:
                    print(f"--- [Main Thread] WARNING: 'data' key missing or not a list in 24hrTicker message: {msg}")
            else:
                print(f"--- [Main Thread] Received UNEXPECTED EVENT TYPE '{msg['e']}': {msg}")
        except queue.Empty:
            break
        except Exception as e:
            print(f"--- [Main Thread] Unexpected error processing message from queue: {e}")


# --- WebSocket Control Functions ---
def start_websocket_manager(symbols):
    """
    Initializes and starts/restarts the WebSocket manager with new subscriptions.
    This function first stops any existing manager, then creates and starts a new one
    to ensure clean subscription updates for the selected symbols.
    """
    print("\n--- STARTING WEBSOCKET MANAGER ---")
    if st.session_state.websocket_manager: 
        print("Existing WebSocket manager found. Attempting to stop it...")
        stop_websocket_manager() 
        print("Existing manager stopped.")

    st.session_state.price_data = {} 
    print("Cleared previous price data.")

    st.session_state.message_queue = queue.Queue()
    print("Re-initialized message queue.")

    st.session_state.websocket_manager = ThreadedWebsocketManager(
        api_key=BINANCE_API_KEY, 
        api_secret=BINANCE_API_SECRET
    )
    print("Calling ThreadedWebsocketManager.start()...")
    st.session_state.websocket_manager.start()
    print("ThreadedWebsocketManager.start() called. This creates the thread.")

    time.sleep(0.1) 
    print("Brief pause (0.1s) completed after manager start.")

    print(f"Attempting to subscribe to individual 24hr mini-ticker streams for {len(symbols)} symbols...")
    for symbol in symbols:
        try:
            callback_with_queue = functools.partial(handle_socket_message, 
                                                    queue_instance=st.session_state.message_queue)
            
            st.session_state.websocket_manager.start_symbol_miniticker_socket(
                callback=callback_with_queue, 
                symbol=symbol
            )
            print(f"Successfully sent subscription request for {symbol} 24hr mini-ticker.")
        except Exception as e:
            print(f"--- ERROR: Failed to send subscription request for {symbol}: {e}")
            st.error(f"Failed to subscribe to {symbol}. Check terminal for details.")

    print("All individual 24hr mini-ticker subscription requests sent.")
    st.session_state.is_websocket_running = True
    st.success(f"Started real-time data stream for: {', '.join(symbols)}. **Check your terminal for connection status and incoming data.**")


def stop_websocket_manager():
    """
    Stops the WebSocket manager and cleans up its thread.
    """
    print("\n--- STOPPING WEBSOCKET MANAGER ---")
    if st.session_state.websocket_manager:
        print("Calling ThreadedWebsocketManager.stop()...")
        st.session_state.websocket_manager.stop()
        print("Calling ThreadedWebsocketManager.join()...")
        st.session_state.websocket_manager.join() 
        st.session_state.websocket_manager = None 
        print("WebSocket manager stopped and joined successfully.")
    else:
        print("No WebSocket manager was running to stop.")
    st.session_state.is_websocket_running = False
    st.info("Stopped real-time data stream.")

# --- Initialization of Binance Client (for initial symbol list) ---
@st.cache_resource
def get_binance_client():
    """
    Returns a Binance client instance, cached to avoid re-creation on reruns.
    Initialized without API keys to access public REST API endpoints (e.g., exchange info).
    """
    if st.session_state.binance_client is None:
        print("Initializing Binance Client (REST API)...")
        st.session_state.binance_client = Client(
            api_key=BINANCE_API_KEY, 
            api_secret=BINANCE_API_SECRET
        )
        print("Binance Client initialized.")
    return st.session_state.binance_client

@st.cache_data(ttl=3600) 
def get_all_trading_symbols(_client): 
    """
    Fetches all available trading pairs (e.g., BTCUSDT, ETHUSDT) using a public REST API endpoint.
    The '_client' argument is prefixed with an underscore so Streamlit's caching
    mechanism does not try to hash this unhashable object.
    """
    print("Fetching all trading symbols from Binance REST API (cached for 1hr)...")
    try:
        exchange_info = _client.get_exchange_info()
        symbols = [s['symbol'] for s in exchange_info['symbols'] if s['status'] == 'TRADING' and 'USDT' in s['symbol']]
        print(f"Successfully fetched {len(symbols)} trading symbols.")
        return sorted(symbols)
    except Exception as e:
        print(f"--- ERROR: Failed to fetch symbols from Binance REST API: {e}")
        st.error(f"Error fetching symbols from Binance: {e}. Please check your internet connection or Binance API status.")
        return []

# --- Streamlit UI Layout ---
st.set_page_config(
    page_title="Binance Real-Time Crypto Data",
    page_icon="📈",
    layout="wide"
)

st.title("📈 Real-Time Crypto Prices (Binance Public API)")
st.write("This dashboard fetches real-time cryptocurrency data using Binance's public WebSocket and REST APIs. "
         "**No API keys are required for this functionality.**")

# Initialize Binance client for initial data (like available symbols)
client = get_binance_client()
all_symbols = get_all_trading_symbols(client)

if not all_symbols:
    st.warning("Could not fetch trading symbols. The application cannot proceed without this data. "
               "Please check your internet connection or Binance API status.")
    st.stop() # Stop the app if symbols can't be loaded

# --- Sidebar for Coin Selection and Controls ---
with st.sidebar:
    st.header("Settings")
    
    default_symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT"] 
    
    selected_symbols = st.multiselect(
        "Select Coins (USDT Pairs):",
        options=all_symbols,
        default=[s for s in default_symbols if s in all_symbols], 
        key="symbol_selector"
    )

    st.markdown("---")
    
    if st.button("Start Real-Time Stream", 
                 key="start_stream_btn", 
                 disabled=st.session_state.is_websocket_running or not selected_symbols):
        if selected_symbols:
            start_websocket_manager(selected_symbols)
        else:
            st.warning("Please select at least one symbol to start the stream.")
            
    if st.button("Stop Stream", 
                 key="stop_stream_btn", 
                 disabled=not st.session_state.is_websocket_running):
        stop_websocket_manager()

    st.markdown("---")
    st.info("Data updates approximately every 1-2 seconds via Binance Public WebSocket.")
    st.markdown("Developed by [Your Name/Org](https://example.com)") # Replace with your info

# --- Main Display Area ---
if not st.session_state.is_websocket_running:
    st.info("Select coins from the sidebar and click 'Start Real-Time Stream'.")

# Placeholder for the data table, which will be updated in place
data_placeholder = st.empty()

# --- Data Display Loop ---
while st.session_state.is_websocket_running:
    process_messages_from_queue()

    if selected_symbols:
        display_data = {symbol: st.session_state.price_data.get(symbol, 
            {'Last Price': 'N/A', '24hr Change %': 'N/A', '24hr High': 'N/A', 
             '24hr Low': 'N/A', '24hr Volume (Base)': 'N/A', '24hr Volume (Quote)': 'N/A', 
             'Last Updated': 'N/A'})
            for symbol in selected_symbols
        }
        
        df = pd.DataFrame.from_dict(display_data, orient='index')
        df.index.name = 'Symbol'

        def color_change(val):
            try:
                val = float(val.replace('%', ''))
                if val > 0:
                    return 'color: green; font-weight: bold;'
                elif val < 0:
                    return 'color: red; font-weight: bold;'
                else:
                    return ''
            except ValueError: 
                return ''

        # This line should now work after upgrading pandas
        styled_df = df.style.map(color_change, subset=['24hr Change %']) 
        
        data_placeholder.dataframe(styled_df, use_container_width=True)
    else:
        data_placeholder.info("No symbols selected. Please select coins from the sidebar to view data.")

    time.sleep(1) 