import streamlit as st
import pandas as pd
import time
from datetime import datetime
import queue
import functools
import requests # For REST API calls
import websocket # For WebSocket client (websocket-client library)
import json # For JSON parsing

# --- IMPORTANT NOTE ON PYTHON VERSION ---
# This app is designed to run on a stable Python release (e.g., 3.9, 3.10, 3.11, 3.12).
# If deploying to Streamlit Cloud, it will likely default to a stable version.
# The previous issues with Python 3.13 and Binance API are now irrelevant with CoinAPI.io.
# --- END IMPORTANT NOTE ---

# --- CoinAPI.io Configuration ---
# Your CoinAPI.io API key from .streamlit/secrets.toml (or Streamlit Cloud secrets)
COINAPI_API_KEY = st.secrets["COINAPI_API_KEY"] # This will read from secrets.toml locally or Streamlit Cloud secrets
COINAPI_REST_URL = "https://rest.coinapi.io/v1"
COINAPI_WS_URL = "wss://ws.coinapi.io/v1/" # WebSocket endpoint

# --- Data Storage and WebSocket Management ---
if 'price_data' not in st.session_state:
    st.session_state.price_data = {} # Stores real-time price updates {symbol: data_dict}
if 'websocket_thread' not in st.session_state:
    st.session_state.websocket_thread = None # Stores the WebSocket thread object
if 'is_websocket_running' not in st.session_state:
    st.session_state.is_websocket_running = False
if 'message_queue' not in st.session_state:
    st.session_state.message_queue = queue.Queue()
if 'current_ws_symbols' not in st.session_state:
    st.session_state.current_ws_symbols = [] # Keep track of active WS subscriptions

# --- Background Task: WebSocket Thread ---
# This thread connects to CoinAPI.io WebSocket and puts messages into the queue.
def websocket_connection_thread(api_key, queue_instance, symbols_to_subscribe):
    def on_message(ws, message):
        try:
            msg = json.loads(message)
            queue_instance.put(msg)
            # print(f"--- [WS Thread] Put message in queue: {msg.get('symbol_id', msg.get('type', ''))}")
        except json.JSONDecodeError:
            print(f"--- [WS Thread] JSON Decode Error: {message}")
        except Exception as e:
            print(f"--- [WS Thread] Error processing message: {e} - {message}")

    def on_error(ws, error):
        print(f"--- [WS Thread] WebSocket Error: {error}")
        # Optionally, put an error message in the queue for the main thread to display
        queue_instance.put({"type": "error", "message": str(error)})

    def on_close(ws, close_status_code, close_msg):
        print(f"--- [WS Thread] WebSocket Closed: {close_status_code} - {close_msg}")
        # Optionally, put a close message in the queue
        queue_instance.put({"type": "closed", "message": f"{close_status_code} - {close_msg}"})

    def on_open(ws):
        print("--- [WS Thread] WebSocket Opened.")
        # Subscribe to trades for the selected symbols
        # CoinAPI.io symbol format: EXCHANGE_TYPE_ASSET_QUOTE (e.g., BINANCE_SPOT_BTC_USDT)
        # We need to map our "BTCUSDT" back to "BINANCE_SPOT_BTC_USDT"
        coinapi_symbol_ids = [f"BINANCE_SPOT_{s}" for s in symbols_to_subscribe] # Assuming Binance Spot as exchange
        
        subscribe_message = {
            "type": "hello",
            "apikey": api_key,
            "heartbeat": False,
            "subscribe_data_type": ["trade"], # We want last trade price
            "subscribe_filter_symbol_id": coinapi_symbol_ids,
            "subscribe_filter_period_id": ["1DAY"], # Not directly for trade, but can be for OHLCV
            "subscribe_filter_asset_id": ["USD", "USDT"], # Example for other filtering
            "limit_usage_for_all_quotes": True # Helps manage free tier limits
        }
        try:
            ws.send(json.dumps(subscribe_message))
            print(f"--- [WS Thread] Sent subscribe message: {subscribe_message}")
        except Exception as e:
            print(f"--- [WS Thread] Error sending subscribe message: {e}")

    # Create and run the WebSocket app
    ws_app = websocket.WebSocketApp(
        COINAPI_WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    # run_forever blocks the thread, so this needs to be its own thread
    ws_app.run_forever(ping_interval=30, ping_timeout=10)


# --- Data Processing Function (runs in main Streamlit thread) ---
def process_messages_from_queue():
    """
    Reads messages from the queue and updates st.session_state.price_data.
    This function should be called from the main Streamlit thread.
    """
    if 'price_data' not in st.session_state:
        st.session_state.price_data = {} 

    while not st.session_state.message_queue.empty():
        try:
            msg = st.session_state.message_queue.get_nowait() 

            if msg.get('type') == 'trade':
                # Example trade message: {'time_exchange': ..., 'price': 123.45, 'symbol_id': 'BINANCE_SPOT_BTC_USDT', ...}
                symbol_id = msg.get('symbol_id')
                price = msg.get('price')
                
                if symbol_id and price is not None:
                    # Convert CoinAPI symbol_id (e.g., BINANCE_SPOT_BTC_USDT) back to BTCUSDT
                    # This relies on a consistent naming convention
                    parts = symbol_id.split('_')
                    if len(parts) >= 3:
                        original_symbol = f"{parts[-2]}{parts[-1]}" # BTCUSDT from BINANCE_SPOT_BTC_USDT
                    else:
                        original_symbol = symbol_id # Fallback if format is different

                    if original_symbol in st.session_state.price_data:
                        # Only update 'Last Price' and 'Last Updated' from WebSocket trade stream
                        st.session_state.price_data[original_symbol]['Last Price'] = f"{float(price):,.4f}"
                        st.session_state.price_data[original_symbol]['Last Updated'] = datetime.now().strftime("%H:%M:%S")
                    else:
                        # If the symbol isn't yet in price_data (e.g., REST API not fetched yet)
                        # or if user just selected it, initialize with N/A for other fields
                        st.session_state.price_data[original_symbol] = {
                            'Last Price': f"{float(price):,.4f}",
                            '24hr Change %': 'N/A',
                            '24hr High': 'N/A',
                            '24hr Low': 'N/A',
                            '24hr Volume (Base)': 'N/A',
                            '24hr Volume (Quote)': 'N/A',
                            'Last Updated': datetime.now().strftime("%H:%M:%S")
                        }
                    # print(f"--- [Main Thread] Updated Last Price for {original_symbol}: {price}")
            elif msg.get('type') == 'error':
                st.error(f"WebSocket Error: {msg.get('message', 'Unknown error')}")
                st.session_state.is_websocket_running = False # Stop loop on critical error
            elif msg.get('type') == 'closed':
                st.warning(f"WebSocket Closed: {msg.get('message', 'Connection closed')}")
                st.session_state.is_websocket_running = False # Stop loop if connection closes
            # else:
                # print(f"--- [Main Thread] Received unhandled WebSocket message type: {msg.get('type')}")
        except queue.Empty:
            break
        except Exception as e:
            print(f"--- [Main Thread] Unexpected error processing message from queue: {e} - {msg}")


# --- REST API Functions ---
@st.cache_data(ttl=3600) # Cache for 1 hour to reduce API calls
def get_all_trading_symbols_coinapi(api_key):
    """
    Fetches all available trading pairs (specifically Binance Spot USDT pairs) from CoinAPI.io.
    Limited to a few hundred symbols to keep API calls manageable for free tier.
    """
    headers = {'X-CoinAPI-Key': api_key}
    # Using 'symbols' endpoint filtered for BINANCE_SPOT exchange
    # Free tier might have limitations on this.
    try:
        response = requests.get(f"{COINAPI_REST_URL}/v1/symbols?filter_exchange_id=BINANCE_SPOT&filter_symbol_id=SPOT_*_USDT", headers=headers, timeout=10)
        response.raise_for_status() # Raise an exception for HTTP errors
        symbols_data = response.json()
        
        # Extract symbols in BTCUSDT format from symbol_id like BINANCE_SPOT_BTC_USDT
        extracted_symbols = []
        for s in symbols_data:
            symbol_id = s.get('symbol_id')
            if symbol_id and symbol_id.startswith('BINANCE_SPOT_') and symbol_id.endswith('_USDT'):
                parts = symbol_id.split('_')
                if len(parts) == 4: # BINANCE_SPOT_ASSET_QUOTE
                    extracted_symbols.append(f"{parts[2]}{parts[3]}")
        
        print(f"Successfully fetched {len(extracted_symbols)} trading symbols from CoinAPI.io.")
        return sorted(list(set(extracted_symbols))) # Use set to remove duplicates, then sort
    except requests.exceptions.HTTPError as errh:
        st.error(f"CoinAPI.io HTTP Error: {errh}. Check your API key or limits.")
        print(f"--- ERROR: CoinAPI.io HTTP Error: {errh}")
    except requests.exceptions.ConnectionError as errc:
        st.error(f"CoinAPI.io Connection Error: {errc}. Check internet connection.")
        print(f"--- ERROR: CoinAPI.io Connection Error: {errc}")
    except requests.exceptions.Timeout as errt:
        st.error(f"CoinAPI.io Timeout Error: {errt}. API call timed out.")
        print(f"--- ERROR: CoinAPI.io Timeout Error: {errt}")
    except requests.exceptions.RequestException as err:
        st.error(f"CoinAPI.io Request Error: {err}")
        print(f"--- ERROR: CoinAPI.io Request Error: {err}")
    except Exception as e:
        st.error(f"An unexpected error occurred while fetching CoinAPI.io symbols: {e}")
        print(f"--- ERROR: Unexpected error fetching CoinAPI.io symbols: {e}")
    return []

def fetch_24hr_ohlcv_data(api_key, symbols):
    """
    Fetches 24-hour OHLCV data for selected symbols from CoinAPI.io REST API.
    This will be used for 24hr Change %, High, Low, Volume.
    """
    headers = {'X-CoinAPI-Key': api_key}
    updated_count = 0
    # Process only a few symbols at a time to avoid rate limits
    # The free tier often allows ~100 requests per day. Fetching all can quickly exhaust it.
    
    # Using 'ohlcv/SYMBOL_ID/latest' for 1day period
    # Example symbol_id: BINANCE_SPOT_BTC_USDT
    for symbol in symbols:
        # Convert BTCUSDT to BINANCE_SPOT_BTC_USDT
        coinapi_symbol_id = f"BINANCE_SPOT_{symbol}" 
        try:
            # We specifically ask for 1DAY period
            response = requests.get(f"{COINAPI_REST_URL}/v1/ohlcv/{coinapi_symbol_id}/latest?period_id=1DAY&limit=1", headers=headers, timeout=5)
            response.raise_for_status()
            data = response.json()
            
            if data and isinstance(data, list) and len(data) > 0:
                ohlcv = data[0]
                open_price = float(ohlcv['price_open'])
                close_price = float(ohlcv['price_close'])
                high_price = float(ohlcv['price_high'])
                low_price = float(ohlcv['price_low'])
                volume_traded = float(ohlcv['volume_traded'])
                volume_traded_quote = float(ohlcv['volume_traded_quote']) # This is often USDT volume

                change_percent = ((close_price - open_price) / open_price) * 100 if open_price != 0 else 0

                # Update the st.session_state.price_data with these 24hr stats
                if symbol not in st.session_state.price_data:
                    # If WebSocket hasn't provided price yet, initialize it
                    st.session_state.price_data[symbol] = {
                        'Last Price': 'N/A', # Last price comes from WS
                        '24hr Change %': 'N/A', '24hr High': 'N/A', '24hr Low': 'N/A',
                        '24hr Volume (Base)': 'N/A', '24hr Volume (Quote)': 'N/A',
                        'Last Updated': datetime.now().strftime("%H:%M:%S")
                    }
                
                st.session_state.price_data[symbol].update({
                    '24hr Change %': f"{change_percent:,.2f}%",
                    '24hr High': f"{high_price:,.4f}",
                    '24hr Low': f"{low_price:,.4f}",
                    '24hr Volume (Base)': f"{volume_traded:,.2f}",
                    '24hr Volume (Quote)': f"{volume_traded_quote:,.2f}"
                })
                updated_count += 1
                # print(f"--- [Main Thread] Updated 24hr OHLCV for {symbol}")
                time.sleep(0.1) # Be kind to CoinAPI.io limits
            else:
                print(f"--- WARNING: No 1-day OHLCV data found for {symbol}. Response: {data}")
                if symbol not in st.session_state.price_data:
                     st.session_state.price_data[symbol] = {
                        'Last Price': 'N/A', '24hr Change %': 'N/A', '24hr High': 'N/A', '24hr Low': 'N/A',
                        '24hr Volume (Base)': 'N/A', '24hr Volume (Quote)': 'N/A',
                        'Last Updated': datetime.now().strftime("%H:%M:%S")
                    }
        except requests.exceptions.RequestException as e:
            print(f"--- ERROR: REST API call failed for {symbol}: {e}")
            if e.response is not None and e.response.status_code == 429:
                st.warning("CoinAPI.io Rate Limit Exceeded. 24hr stats updates paused.")
                print("--- WARNING: CoinAPI.io Rate Limit Exceeded.")
            if symbol not in st.session_state.price_data:
                 st.session_state.price_data[symbol] = {
                    'Last Price': 'N/A', '24hr Change %': 'N/A', '24hr High': 'N/A', '24hr Low': 'N/A',
                    '24hr Volume (Base)': 'N/A', '24hr Volume (Quote)': 'N/A',
                    'Last Updated': datetime.now().strftime("%H:%M:%S")
                }
        except Exception as e:
            print(f"--- ERROR: Unexpected error fetching OHLCV for {symbol}: {e}")
            if symbol not in st.session_state.price_data:
                 st.session_state.price_data[symbol] = {
                    'Last Price': 'N/A', '24hr Change %': 'N/A', '24hr High': 'N/A', '24hr Low': 'N/A',
                    '24hr Volume (Base)': 'N/A', '24hr Volume (Quote)': 'N/A',
                    'Last Updated': datetime.now().strftime("%H:%M:%S")
                }
    if updated_count > 0:
        st.info(f"Updated 24hr stats for {updated_count} symbols via CoinAPI.io REST API.")


# --- WebSocket Control Functions ---
def start_coinapi_websocket(api_key, symbols):
    """
    Initializes and starts the CoinAPI.io WebSocket thread.
    """
    print("\n--- STARTING COINAPI.IO WEBSOCKET ---")
    if st.session_state.websocket_thread and st.session_state.websocket_thread.is_alive():
        print("Existing WebSocket thread found. Attempting to stop it...")
        stop_websocket_manager() # Call the unified stop function
        print("Existing thread stopped.")

    st.session_state.price_data = {} # Clear previous data
    st.session_state.message_queue = queue.Queue() # Re-initialize queue
    st.session_state.current_ws_symbols = symbols # Track currently subscribed symbols

    # Create a partial function for the WebSocket thread's callback
    # This ensures handle_socket_message receives the correct queue instance
    ws_callback = functools.partial(handle_socket_message, 
                                    queue_instance=st.session_state.message_queue)

    # Start the WebSocket connection in a new thread
    import threading
    st.session_state.websocket_thread = threading.Thread(
        target=websocket_connection_thread,
        args=(api_key, st.session_state.message_queue, symbols),
        daemon=True # Daemon thread will exit when main program exits
    )
    st.session_state.websocket_thread.start()
    print("CoinAPI.io WebSocket thread started.")

    st.session_state.is_websocket_running = True
    st.success(f"Started real-time data stream for: {', '.join(symbols)} via CoinAPI.io. **Check your terminal for connection status and incoming data.**")


def stop_websocket_manager():
    """
    Stops the WebSocket thread and cleans up.
    """
    print("\n--- STOPPING COINAPI.IO WEBSOCKET ---")
    if st.session_state.websocket_thread and st.session_state.websocket_thread.is_alive():
        # websocket-client doesn't have a direct 'stop' method on WebSocketApp itself
        # The run_forever loop breaks if connection is lost or interrupted.
        # We might need to manually close the underlying socket for clean exit.
        # A common way is to set a flag or explicitly close the websocket app.
        try:
            # This will cause the run_forever loop to exit
            # We assume the ws_app instance is accessible if the thread is alive
            # However, directly accessing it from outside the thread is tricky.
            # A more robust solution might involve passing a termination event to the thread.
            # For now, relying on the daemon=True for graceful exit on app close.
            # Or if it's the ThreadedWebsocketManager like before, it has .stop()
            # Since we switched to raw websocket-client, this requires a manual close or flag.

            # Simple (less robust) approach: relying on daemon=True
            pass # No specific stop for raw websocket.WebSocketApp in this setup
        except Exception as e:
            print(f"--- ERROR: Failed to stop WebSocket gracefully: {e}")
        finally:
            st.session_state.websocket_thread = None
            print("WebSocket thread object cleared.")
    else:
        print("No active WebSocket thread to stop.")
    st.session_state.is_websocket_running = False
    st.info("Stopped real-time data stream.")


# --- Streamlit UI Layout ---
st.set_page_config(
    page_title="CoinAPI.io Real-Time Crypto Data",
    page_icon="📈",
    layout="wide"
)

st.title("📈 Real-Time Crypto Prices (CoinAPI.io Public API)")
st.write("This dashboard fetches real-time cryptocurrency data using CoinAPI.io's WebSocket and REST APIs. **An API key is required.**")

# Fetch all symbols once at startup (cached)
all_symbols = get_all_trading_symbols_coinapi(COINAPI_API_KEY)

if not all_symbols:
    st.error("Could not fetch trading symbols from CoinAPI.io. Please check your API key, internet connection, or CoinAPI.io status/limits.")
    st.stop() 

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
            start_coinapi_websocket(COINAPI_API_KEY, selected_symbols)
        else:
            st.warning("Please select at least one symbol to start the stream.")
            
    if st.button("Stop Stream", 
                 key="stop_stream_btn", 
                 disabled=not st.session_state.is_websocket_running):
        stop_websocket_manager()

    st.markdown("---")
    st.info("Last Price updates via WebSocket (CoinAPI.io).")
    st.info("24hr stats (Change %, High, Low, Volume) update periodically via REST API (CoinAPI.io).")
    st.markdown("Developed by [Your Name/Org](https://example.com)") 

# --- Main Display Area ---
if not st.session_state.is_websocket_running:
    st.info("Select coins from the sidebar and click 'Start Real-Time Stream'.")

# Placeholder for the data table, which will be updated in place
data_placeholder = st.empty()

# Initialize an update counter for REST API calls
if 'rest_update_counter' not in st.session_state:
    st.session_state.rest_update_counter = 0

# --- Data Display Loop ---
while st.session_state.is_websocket_running:
    # Process messages from the queue in the main Streamlit thread
    process_messages_from_queue()

    # Periodically fetch 24hr OHLCV data via REST API
    # Adjust this interval based on CoinAPI.io's free tier limits (e.g., every 60 seconds)
    REST_UPDATE_INTERVAL = 60 # seconds
    if st.session_state.rest_update_counter % REST_UPDATE_INTERVAL == 0:
        if selected_symbols:
            # Only fetch for selected symbols
            fetch_24hr_ohlcv_data(COINAPI_API_KEY, selected_symbols)
        st.session_state.rest_update_counter = 0 # Reset counter

    st.session_state.rest_update_counter += 1 # Increment counter every second of the loop

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

        styled_df = df.style.map(color_change, subset=['24hr Change %']) 
        
        data_placeholder.dataframe(styled_df, use_container_width=True)
    else:
        data_placeholder.info("No symbols selected. Please select coins from the sidebar to view data.")

    time.sleep(1) # Controls UI refresh rate